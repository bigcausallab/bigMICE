#' Random Forest Regression Imputation function
#'
#' This function imputes missing values in a Spark DataFrame using Random Forest regression.
#' @importFrom dplyr %>%
#'
#' @param sc A Spark connection
#' @param sdf A Spark DataFrame
#' @param target_col The column with missing values to impute
#' @param feature_cols The columns to use as features in the Random Forest regression model. These columns should not have missing values.
#' @param target_col_prev the target column at the previous iteration. Used to calculate residuals.
#' @return The Spark DataFrame with missing values imputed in the target column
#' @export
#' @examples
#' #TBD

impute_with_random_forest_regressor <- function(sc, sdf, target_col, feature_cols, target_col_prev) {
  # Random forest regressor using sparklyr ml_random_forest Good for continuous values
  # Doc: https://rdrr.io/cran/sparklyr/man/ml_random_forest.html

  #TODO: Added more flexibility for the user to use hyperparameters of the model (see doc)
  # Maybe add that as a ... param to the function
  if (!is.character(target_col) || length(target_col) != 1) {
    stop("target_col must be a single column name as a character string")
  }
  if (!is.character(feature_cols) || length(feature_cols) == 0) {
    stop("feature_cols must be a character vector of column names")
  }
  #Step 1: add temporary id
  sdf <- sdf %>% sparklyr::sdf_with_sequential_id()
  target_col_prev <- target_col_prev %>% sparklyr::sdf_with_sequential_id()

  # Step 2: Split the data into complete and incomplete rows
  # Reminder: all non target columns will have been initialized
  complete_data <- sdf %>%
    dplyr::filter(!is.na(!!rlang::sym(target_col)))

  incomplete_data <- sdf %>%
    dplyr::filter(is.na(!!rlang::sym(target_col)))
  n_incomplete <- sparklyr::sdf_nrow(incomplete_data)
  print(n_incomplete)
  if(n_incomplete == 0){
    print("NO MISSING VALUES, SKIPPING MODEL BUILDING")
    return(sdf %>% dplyr::select(-dplyr::all_of("id")))
  }
  # Step 3: Build regression formula
  formula_str <- paste0(target_col, " ~ ", paste(feature_cols, collapse = " + "))
  formula_obj <- stats::as.formula(formula_str)

  # Step 4: Build logistic regression model on complete data
  model <- complete_data %>%
    sparklyr::ml_random_forest_regressor(formula = formula_obj)

  # Step 5: Predict missing values
  predictions <- sparklyr::ml_predict(model, incomplete_data) %>%
    sparklyr::sdf_with_sequential_id("pred_id")

  pred_residuals <- predictions %>%
    sparklyr::inner_join(target_col_prev, by = "id")

  sd_res <- pred_residuals %>%
    sparklyr::mutate(residuals = (prediction - !!rlang::sym(paste0(target_col,"_y")))^2)

  sd_res <- sd_res %>% dplyr::summarise(res_mean = mean(residuals, na.rm = TRUE)) %>% collect()
  sd_res <- sqrt(sd_res[[1, 1]])

  # Add noise to prediction to account for uncertainty
  n_pred <- sparklyr::sdf_nrow(predictions)
  noise_sdf <- sparklyr::sdf_rnorm(sc = sc, n = n_pred, sd = sd_res, output_col = "noise") %>%
    sparklyr::sdf_with_sequential_id("pred_id")

  #Join the noise and the prediction
  predictions <- predictions %>% inner_join(noise_sdf, by="pred_id") %>%
    dplyr::select(-all_of("pred_id")) %>%
    sparklyr::mutate(noisy_pred = prediction + noise) %>%
    dplyr::select(-all_of(c("prediction","noise")))

  # Replace the NULL values with predictions
  incomplete_data <- predictions %>%
    dplyr::select(-!!rlang::sym(target_col)) %>%  # Remove the original NULL column
    dplyr::rename(!!rlang::sym(target_col) := noisy_pred)  # Rename prediction to target_col

  # Step 6: Combine complete and imputed data
  result <- complete_data %>%
    dplyr::union_all(incomplete_data)

  result <- result %>%
    dplyr::arrange(id) %>%
    dplyr::select(-id)

  return(result)
}
#' Random Forest Classification Imputation function
#'
#' This function imputes missing values in a Spark DataFrame using Random Forest classification.
#'
#' @param sc A Spark connection
#' @param sdf A Spark DataFrame
#' @param target_col The column with missing values to impute
#' @param feature_cols The columns to use as features in the Random Forest regression model. These columns should not have missing values.
#' @return The Spark DataFrame with missing values imputed in the target column
#' @export
#' @examples
#' #TBD

impute_with_random_forest_classifier <- function(sc, sdf, target_col, feature_cols) {
  # Random forest imputer using sparklyr ml_random_forest Good for categorical values
  # Doc: https://rdrr.io/cran/sparklyr/man/ml_random_forest.html

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
  print(n_incomplete)
  if(n_incomplete == 0){
    print("NO MISSING VALUES, SKIPPING MODEL BUILDING")
    return(sdf %>% dplyr::select(-dplyr::all_of("id")))
  }
  # Step 3: Build regression formula
  formula_str <- paste0(target_col, " ~ ", paste(feature_cols, collapse = " + "))
  formula_obj <- stats::as.formula(formula_str)

  # Step 4: Build logistic regression model on complete data
  model <- complete_data %>%
    sparklyr::ml_random_forest_classifier(formula = formula_obj)

  # Step 5: Predict missing values
  predictions <- sparklyr::ml_predict(model, incomplete_data)


  # At this point , predictions$prediction holds the predicted values without taking into account uncertainty.
  # To take into account the predictive uncertainty, we need to extract the probabilities
  # Step 1: Generate random uniform values and add them to the sdf
  n_missing <- predictions %>% count() %>% collect() %>% pull()
  runif_values <- sparklyr::sdf_runif(sc, n_missing,output_col = "runif") %>%
    sparklyr::sdf_with_sequential_id(id = "temp_id_runif")

  predictions <- predictions %>%
    sparklyr::sdf_with_sequential_id(id = "temp_id_runif") %>%
    left_join(runif_values, by = "temp_id_runif") %>%
    select(-temp_id_runif)

  # Step 2: Extract the class names from the probability columns
  # This step is done because the classes might not always be ordered numbers
  classes <- colnames(predictions %>% select(starts_with("probability_"))) %>%
    sub(pattern = "probability_", replacement = "")

  cat("LogReg - DEBUG: class names = ", classes)

  # Step 3: Generate the cumulative probability columns:
  for (i in seq_along(classes)) {
    class_subset <- classes[1:i]
    prob_cols <- paste0("probability_", class_subset)
    cumprob_col <- paste0("cumprob_", classes[i])

    # Spark doesn't allow row-wise, so we add columns using SQL expression
    expr <- paste(prob_cols, collapse = " + ")

    predictions <- predictions %>%
      mutate(!!cumprob_col := sql(expr))
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

  # Add prob_pred column using SQL expression:
  predictions <- predictions %>% mutate(prob_pred = sql(case_when_sql))

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

