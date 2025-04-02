#' Linear Regression Imputation function
#'
#' This function imputes missing values in a Spark DataFrame using linear regression.
#'
#' @param sc A Spark connection
#' @param sdf A Spark DataFrame
#' @param target_col The column with missing values to impute
#' @param feature_cols The columns to use as features in the linear regression model. These columns should not have missing values.
#' @param elastic_net_param The elastic net parameter for the linear regression model. Default is 0 (ridge regression)
#' @return The Spark DataFrame with missing values imputed in the target column
#' @export
#' @examples
#' #TBD

impute_with_linear_regression <- function(sc, sdf, target_col, feature_cols, elastic_net_param = 0) {
  # Given a spark connection, a spark dataframe, a target column with missing values,
  # and feature columns without missing values, this function:
  # 1. Builds a linear regression model using complete cases
  # 2. Uses that model to predict missing values
  # 3. Returns a dataframe with imputed values in the target column

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

  # Step 3: Build regression formula
  formula_str <- paste0(target_col, " ~ ", paste(feature_cols, collapse = " + "))
  formula_obj <- as.formula(formula_str)

  # Step 4: Build linear regression model on complete data
  lm_model <- complete_data %>%
    sparklyr::ml_linear_regression(formula = formula_obj,
                         elastic_net_param = elastic_net_param)

  # Step 5: Predict missing values
  predictions <- sparklyr::ml_predict(lm_model, incomplete_data)

  # Replace the NULL values with predictions
  incomplete_data <- predictions %>%
    dplyr::select(-!!rlang::sym(target_col)) %>%  # Remove the original NULL column
    dplyr::rename(!!rlang::sym(target_col) := prediction)  # Rename prediction to target_col

  # Re join the observed and imputed rows
  result <- complete_data %>%
    dplyr::union_all(incomplete_data)

  # Restore original row order and return
  result <- result %>%
    dplyr::arrange(id) %>%
    dplyr::select(-id)

  return(result)
}
