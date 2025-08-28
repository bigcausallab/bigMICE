#' Logistic Regression Imputation function
#'
#' This function imputes missing values in a Spark DataFrame using logistic regression. This function is intended for boolean variables only (0/1).
#' @importFrom dplyr %>%
#' @importFrom data.table :=
#'
#' @param sc A Spark connection
#' @param sdf A Spark DataFrame
#' @param target_col The column with missing values to impute
#' @param feature_cols The columns to use as features in the logistic regression model. These columns should not have missing values.
#' @return The Spark DataFrame with missing values imputed in the target column
#' @examples
#' # Create a dataset with missing boolean values
#' #library(sparklyr)
#' #library(dplyr)
#'
#' # Connect to Spark
#' #sc <- spark_connect(master = "local")
#'
#' # Create sample data with missing values in a boolean column 'has_degree'
#' #sample_data <- data.frame(
#' #  has_degree = c(1, NA, 0, NA, 1, 0),
#' #  age = c(25, 35, 30, 28, 45, 22),
#' #  income = c(50000, 75000, 45000, 52000, 90000, 35000),
#' #  years_experience = c(2, 8, 5, 3, 15, 1)
#' #)
#'
#' # Copy to Spark DataFrame
#' #sdf <- copy_to(sc, sample_data, "sample_data")
#'
#' # Impute missing boolean values using age, income, and experience
#' #imputed_sdf <- impute_with_logistic_regression(
#' #  sc = sc,
#' #  sdf = sdf,
#' #  target_col = "has_degree",
#' #  feature_cols = c("age", "income", "years_experience")
#' #)
#'
#' # View results
#' #imputed_sdf %>% collect()
#'
#' # Clean up
#' #spark_disconnect(sc)
#' @export

impute_with_logistic_regression <- function(sc, sdf, target_col, feature_cols) {

  # Step 0; Validate inputs
  if (!is.character(target_col) || length(target_col) != 1) {
    stop("target_col must be a single column name as a character string")
  }
  if (!is.character(feature_cols) || length(feature_cols) == 0) {
    stop("feature_cols must be a character vector of column names")
  }
  #Step 1: add temporary id
  sdf <- sdf %>% sparklyr::sdf_with_sequential_id()

  # Step 2: Split the data into complete and incomplete rows
  # Reminder: all non target columns will have been initialized
  complete_data <- sdf %>%
    dplyr::filter(!is.na(!!rlang::sym(target_col)))

  incomplete_data <- sdf %>%
    dplyr::filter(is.na(!!rlang::sym(target_col)))
  n_incomplete <- sparklyr::sdf_nrow(incomplete_data)
  #print(n_incomplete)
  if(n_incomplete == 0){
    cat("- No missing values, skipping imputation")
    return(sdf %>% dplyr::select(-dplyr::all_of("id")))
  }

  # Step 3: Build regression formula
  formula_str <- paste0(target_col, " ~ ", paste(feature_cols, collapse = " + "))
  formula_obj <- stats::as.formula(formula_str)

  # Step 4: Build logistic regression model on complete data
  # TODO: add the possibility to specify more model hyperparameters (do.call(...))
  model <- complete_data %>%
    sparklyr::ml_logistic_regression(formula = formula_obj)

  # Step 5: Predict missing values
  predictions <- sparklyr::ml_predict(model, incomplete_data)
  # Note to self: If getting a stringNULL error when using string label, remove target_col from incomplete data (see mult_logistic) and comment line 126  dplyr::select(-!!rlang::sym(target_col)) %>%

  # At this point , predictions$prediction holds the predicted values without taking into account uncertainty.
  # To take into account the predictive uncertainty, we need to extract the probabilities
  # Step 1: Generate random uniform values and add them to the sdf
  n_missing <- predictions %>% dplyr::count() %>% dplyr::collect() %>% dplyr::pull()
  runif_values <- sparklyr::sdf_runif(sc, n_missing,output_col = "runif") %>%
    sparklyr::sdf_with_sequential_id(id = "temp_id_runif")

  predictions <- predictions %>%
    sparklyr::sdf_with_sequential_id(id = "temp_id_runif") %>%
    dplyr::left_join(runif_values, by = "temp_id_runif") %>%
    dplyr::select(-temp_id_runif)

  # Step 2: Extract the class names from the probability columns
  # This step is done because the classes might not always be ordered numbers
  classes <- colnames(predictions %>% dplyr::select(dplyr::starts_with("probability_"))) %>%
    sub(pattern = "probability_", replacement = "")

  # Make this optional ?
  #cat("LogReg - DEBUG: class names = ", classes)

  # Step 3: Generate the cumulative probability columns:
  for (i in seq_along(classes)) {
    class_subset <- classes[1:i]
    prob_cols <- paste0("probability_", class_subset)
    cumprob_col <- paste0("cumprob_", classes[i])

    # Spark doesn't allow row-wise, so we add columns using SQL expression
    expr <- paste(prob_cols, collapse = " + ")

    predictions <- predictions %>%
      dplyr::mutate(!!cumprob_col := dplyr::sql(expr))
  }
  # Step 4: Add the probabilistic prediction using runif and cumprob_ columns
  # Again here, use of SQL expressions.

  # Build case_when conditions as SQL snippets:
  case_when_sql <- paste0(
    "WHEN runif <= ", paste0("cumprob_", classes[1]), " THEN '", classes[1], "' "
  )

  if(length(classes) > 1){
    for(i in 2:length(classes)){
      cond <- paste0("WHEN runif > ", paste0("cumprob_", classes[i-1]),
                     " AND runif <= ", paste0("cumprob_", classes[i]),
                     " THEN '", classes[i], "' ")
      case_when_sql <- paste0(case_when_sql, cond)
    }
  }

  # Add ELSE clause for safety (optional):
  case_when_sql <- paste0("CASE ", case_when_sql, " ELSE NULL END")

  # Add prob_pred column using SQL expression:
  predictions <- predictions %>% dplyr::mutate(prob_pred = dplyr::sql(case_when_sql))

  # At this point, the column prob_pred contains the predictions that take into account the predictive uncertainty


  # removing columns created during procedure
  pre_pred_cols <- c(colnames(incomplete_data),"prob_pred")
  post_pred_cols <- colnames(predictions)
  extra_cols <- setdiff(post_pred_cols, pre_pred_cols)
  predictions <- predictions %>% dplyr::select(-dplyr::all_of(extra_cols))

  # Replace the NULL values with predictions
  incomplete_data <- predictions %>%
    dplyr::select(-!!rlang::sym(target_col)) %>%  # Remove the original NULL column
    # dplyr::mutate(prediction = as.logical(prediction)) %>%
    dplyr::rename(!!rlang::sym(target_col) := prob_pred)  # Rename prediction to target_col

  # Step 6: Combine complete and imputed data
  result <- complete_data %>%
    dplyr::union_all(incomplete_data)

  result <- result %>%
    dplyr::arrange(id) %>%
    dplyr::select(-id)

  return(result)
}
