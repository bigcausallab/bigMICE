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
#' @examples
#' # Create a simple dataset with missing values
#' #library(sparklyr)
#' #library(dplyr)
#'
#' # Connect to Spark
#' # sc <- spark_connect(master = "local")
#'
#' # Create sample data with some missing values in 'age'
#' #sample_data <- data.frame(
#' # age = c(25, NA, 35, NA, 45, 30),
#' # income = c(50000, 60000, 70000, 55000, 80000, 52000),
#' # education_years = c(16, 18, 20, 17, 22, 16),
#' # experience = c(3, 8, 12, 5, 18, 7)
#' #)
#'
#' # Copy to Spark DataFrame
#' #sdf <- copy_to(sc, sample_data, "sample_data")
#'
#' # Create previous iteration data (for residual calculation)
#' # In practice, this would be from a previous imputation step
#' #sdf_prev <- sdf %>%
#' #  mutate(age_prev = ifelse(is.na(age), 30, age)) %>%  # Simple initial imputation
#' #  select(age_prev)
#'
#' # Impute missing age values using income, education_years, and experience
#' #imputed_sdf <- impute_with_linear_regression(
#' #  sc = sc,
#' #  sdf = sdf,
#' #  target_col = "age",
#' #  feature_cols = c("income", "education_years", "experience"),
#' #  elastic_net_param = 0,  # Ridge regression
#' #  target_col_prev = sdf_prev
#' #)
#'
#' # View results
#' #imputed_sdf %>% collect()
#'
#' # Clean up
#' #spark_disconnect(sc)
#' @export
impute_with_linear_regression <- function(sc, sdf, target_col, feature_cols, elastic_net_param = 0,target_col_prev) {

  # Step 0; Validate inputs
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

  if(n_incomplete == 0){
    cat("- No missing values, skipping imputation")
    return(sdf %>% dplyr::select(-dplyr::all_of("id")))
  }

  # Step 3: Build regression formula
  formula_str <- paste0(target_col, " ~ ", paste(feature_cols, collapse = " + "))
  formula_obj <- stats::as.formula(formula_str)

  # Step 4: Build linear regression model on complete data
  model <- complete_data %>%
    sparklyr::ml_linear_regression(formula = formula_obj,
                         elastic_net_param = elastic_net_param)

  # TODO: combine the predicts and seperate after the train/test predictions
  # Step 5: Predict missing values (test)
  incomplete_predictions <- sparklyr::ml_predict(model, incomplete_data) %>%
    sparklyr::sdf_with_sequential_id("pred_id")

  # Step 6: Also predict the observed values. (Train)
  # The residuals of these predictions are used to estimate RMSE
  complete_predictions <- sparklyr::ml_predict(model, complete_data) %>%
    sparklyr::sdf_with_sequential_id("rmse_id") #needed ?
  # Join the
  pred_residuals <- complete_predictions %>%
    dplyr::inner_join(target_col_prev, by = "id")

  # plot "prediction" versus "!!rlang::sym(paste0(target_col,"_y")"
  # print(ggplot2::ggplot(pred_residuals, ggplot2::aes(x = prediction, y = !!rlang::sym(paste0(target_col,"_y")))) + ggplot2::geom_point() + ggplot2::labs(title = paste("prediction versus", paste0(target_col,"_y")), x = "prediction", y = paste0(target_col,"_y")))


  sd_res <- pred_residuals %>%
    sparklyr::mutate(residuals = (prediction - !!rlang::sym(paste0(target_col,"_y")))^2)

  #then the difference !!rlang::sym(paste0(target_col,"_y")-prediction versus prediction
  # print(pred_residuals %>% dplyr::mutate(diff = !!rlang::sym(paste0(target_col,"_y")) - prediction) %>% ggplot2::ggplot(ggplot2::aes(x = diff, y = prediction)) + ggplot2::geom_point() + ggplot2::labs(title = paste("difference", paste0(target_col,"_y"), "- prediction versus prediction"), x = "difference", y = "prediction"))

  sd_res <- sd_res %>% dplyr::summarise(res_mean = mean(residuals, na.rm = TRUE)) %>% dplyr::collect()

  sd_res <- sqrt(sd_res[[1, 1]])
  cat("- RMSE residuals:", sd_res," -")

  # Add noise to prediction to account for uncertainty
  n_pred <- sparklyr::sdf_nrow(incomplete_predictions)
  noise_sdf <- sparklyr::sdf_rnorm(sc = sc, n = n_pred, mean = 0, sd = sd_res, output_col = "noise") %>%
    sparklyr::sdf_with_sequential_id("pred_id")

  #Join the noise and the prediction
  incomplete_predictions <- incomplete_predictions %>% dplyr::inner_join(noise_sdf, by="pred_id") %>%
    dplyr::select(-dplyr::all_of("pred_id")) %>%
    sparklyr::mutate(noisy_pred = prediction + noise) %>%
    dplyr::select(-dplyr::all_of(c("prediction","noise"))) # removing of original prediction and individual noise added. only "noisy_pred" remains

  # Replace the NULL values with predictions
  incomplete_data <- incomplete_predictions %>%
    dplyr::select(-!!rlang::sym(target_col)) %>%  # Remove the original NULL column
    dplyr::rename(!!rlang::sym(target_col) := noisy_pred)  # Rename noisy_prediction to target_col name

  # Re join the observed and imputed rows
  result <- complete_data %>%
    dplyr::union_all(incomplete_data)

  # Restore original row order and return
  result <- result %>%
    dplyr::arrange(id) %>%
    dplyr::select(-id)

  return(result)
}
