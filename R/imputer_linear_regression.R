#' Linear Regression Imputation function
#'
#' This function imputes missing values in a Spark DataFrame using linear regression.
#' @importFrom dplyr %>%
#' @importFrom data.table :=
#'
#' @param sc A Spark connection
#' @param sdf A Spark DataFrame
#' @param target_col The column with missing values to impute
#' @param feature_cols The columns to use as features in the linear regression model. These columns should not have missing values.
#' @param elastic_net_param The elastic net parameter for the linear regression model. Default is 0 (ridge regression)
#' @param target_col_prev the target column at the previous iteration. Used to calculate residuals.
#' @return The Spark DataFrame with missing values imputed in the target column
#' @export
#' @examples
#' #TBD

impute_with_linear_regression <- function(sc, sdf, target_col, feature_cols, elastic_net_param = 0,target_col_prev) {
  print("DEBUGlinear: 0")
  # Step 0; Validate inputs
  if (!is.character(target_col) || length(target_col) != 1) {
    stop("target_col must be a single column name as a character string")
  }
  if (!is.character(feature_cols) || length(feature_cols) == 0) {
    stop("feature_cols must be a character vector of column names")
  }
  print("DEBUGlinear: 1")
  #Step 1: add temporary id
  sdf <- sdf %>% sparklyr::sdf_with_sequential_id()
  target_col_prev <- target_col_prev %>% sparklyr::sdf_with_sequential_id()
  print(target_col_prev)
  print("DEBUGlinear: 2")
  # Step 2: Split the data into complete and incomplete rows
  # Reminder: all non target columns will have been initialized
  complete_data <- sdf %>%
    dplyr::filter(!is.na(!!rlang::sym(target_col)))

  incomplete_data <- sdf %>%
    dplyr::filter(is.na(!!rlang::sym(target_col)))
  print("DEBUGlinear: 3")
  # Step 3: Build regression formula
  formula_str <- paste0(target_col, " ~ ", paste(feature_cols, collapse = " + "))
  formula_obj <- stats::as.formula(formula_str)
  print("DEBUGlinear: 4")
  # Step 4: Build linear regression model on complete data
  lm_model <- complete_data %>%
    sparklyr::ml_linear_regression(formula = formula_obj,
                         elastic_net_param = elastic_net_param)
  print("DEBUGlinear: 5")
  # Step 5: Predict missing values
  predictions <- sparklyr::ml_predict(lm_model, incomplete_data) %>%
    sparklyr::sdf_with_sequential_id("pred_id")
  print(predictions)
  print("DEBUGlinear: 5.1")
  pred_residuals <- predictions %>%
    sparklyr::inner_join(target_col_prev, by = "id")
  print(pred_residuals)
  print("DEBUGlinear: 5.2")
  sd_res <- pred_residuals %>%
    sparklyr::mutate(residuals = (prediction - !!rlang::sym(paste0(target_col,"_y")))^2)
  print(sd_res)
  print("DEBUGlinear: 5.3")
  sd_res <- sd_res %>% dplyr::summarise(res_mean = mean(residuals, na.rm = TRUE)) %>% collect()
  print("DEBUGlinear: 5.4")
  sd_res <- sqrt(sd_res[[1, 1]])
  print('sd_res')
  print(sd_res)
  print("DEBUGlinear: 5.5")
  # Add noise to prediction to account for uncertainty
  n_pred <- sparklyr::sdf_nrow(predictions)
  print("n_pred")
  print(n_pred)
  noise_sdf <- sparklyr::sdf_rnorm(sc = sc, n = n_pred, sd = sd_res, output_col = "noise") %>%
    sparklyr::sdf_with_sequential_id("pred_id")
  print("noise_sdf_rows")
  print(sparklyr::sdf_nrow(noise_sdf))
  print("DEBUGlinear: 5.6")
  #Join the noise and the prediction
  predictions <- predictions %>% inner_join(noise_sdf, by="pred_id") %>%
    dplyr::select(-all_of("pred_id")) %>%
    sparklyr::mutate(noisy_pred = prediction + noise) %>%
    dplyr::select(-all_of(c("prediction","noise")))
  print("DEBUGlinear: 6")
  # Replace the NULL values with predictions
  incomplete_data <- predictions %>%
    dplyr::select(-!!rlang::sym(target_col)) %>%  # Remove the original NULL column
    dplyr::rename(!!rlang::sym(target_col) := noisy_pred)  # Rename prediction to target_col

  # Re join the observed and imputed rows
  result <- complete_data %>%
    dplyr::union_all(incomplete_data)
  print("DEBUGlinear: 7")
  # Restore original row order and return
  result <- result %>%
    dplyr::arrange(id) %>%
    dplyr::select(-id)

  return(result)
}
