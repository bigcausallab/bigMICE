#' Multinomial Logistic Regression Imputation function
#'
#' This function imputes missing values in a Spark DataFrame using Multinomial Logistic regression.
#'
#' @param sc A Spark connection
#' @param sdf A Spark DataFrame
#' @param target_col The column with missing values to impute
#' @param feature_cols The columns to use as features in the multinomial logistic regression model. These columns should not have missing values.
#' @return The Spark DataFrame with missing values imputed in the target column
#' @export
#' @examples
#' #TBD

impute_with_mult_logistic_regression <- function(sc, sdf, target_col, feature_cols) {
  # Given a spark connection, a spark dataframe, a target column with missing values,
  # and feature columns without missing values, this function:
  # 1. Builds a multinomial logistic regression model using complete cases
  # 2. Uses that model to predict missing values
  # 3. Returns a dataframe with imputed values in the target column

  if (!is.character(target_col) || length(target_col) != 1) {
    stop("target_col must be a single column name as a character string")
  }
  if (!is.character(feature_cols) || length(feature_cols) == 0) {
    stop("feature_cols must be a character vector of column names")
  }
  #Step 1: add temporary id
  print("DEBUG SDF PreID")
  print(sdf)
  sdf <- sdf %>% sparklyr::sdf_with_sequential_id()

  # Step 2: Split the data into complete and incomplete rows
  # Reminder: all non target columns will have been initialized
  complete_data <- sdf %>%
    dplyr::filter(!is.na(!!rlang::sym(target_col)))

  incomplete_data <- sdf %>%
    dplyr::filter(is.na(!!rlang::sym(target_col)))
  n_incomplete <- sparklyr::sdf_nrow(incomplete_data)

  if(n_incomplete == 0){
    cat("- No missing values, skipping imputation")
    return(sdf %>% dplyr::select(-dplyr::all_of("id")))
  }
  # Step 3: Build regression formula
  formula_str <- paste0(target_col, " ~ ", paste(feature_cols, collapse = " + "))
  formula_obj <- stats::as.formula(formula_str)

  # cat("Checking for NULLs in complete_data:\n")
  # print(complete_data, n=1000)

  # Step 4: Build logistic regression model on complete data
  model <- complete_data %>%
    sparklyr::ml_logistic_regression(formula = formula_obj)

  # Step 5: Predict missing values
  predictions <- sparklyr::ml_predict(model, incomplete_data)

  print(colnames(predictions))
  print(predictions)
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

  cat("LogReg - DEBUG: class names = ", classes,"\n")

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
  # Again here, use of SQL expressions. I used the help of generative AI so I don't fully understand that part, but it looks like it is working.

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
  print(case_when_sql)
  # Add prob_pred column using SQL expression:
  predictions <- predictions %>% dplyr::mutate(prob_pred = dplyr::sql(case_when_sql))

  print("debug predictions")
  print(predictions)
  # At this point, the column prob_pred contains the predictions that take into account the predictive uncertainty


  # removing unused created columns (only need prediction)
  pre_pred_cols <- c(colnames(incomplete_data),"prediction")
  post_pred_cols <- colnames(predictions)
  extra_cols <- setdiff(post_pred_cols, pre_pred_cols)
  predictions <- predictions %>% dplyr::select(-dplyr::all_of(extra_cols))
  print("debug predictions")
  print(predictions)
  # Replace the NULL values with predictions
  incomplete_data <- predictions %>%
    dplyr::select(-!!rlang::sym(target_col)) %>%  # Remove the original NULL column
    #dplyr::mutate(prediction = as.logical(prediction)) %>%
    dplyr::rename(!!rlang::sym(target_col) := prediction)  # Rename prediction to target_col

  print("debug incomplete")
  print(incomplete_data)
  # Step 6: Combine complete and imputed data
  result <- complete_data %>%
    dplyr::union_all(incomplete_data)

  result <- result %>%
    dplyr::arrange(id) %>%
    dplyr::select(-id)

  print("debug return")
  print(result)
  return(result)
}
