#' Random Forest Regression Imputation function
#'
#' This function imputes missing values in a Spark DataFrame using Random Forest regression.
#' @importFrom dplyr %>%
#'
#' @param sc A Spark connection
#' @param sdf A Spark DataFrame
#' @param target_col The column with missing values to impute
#' @param feature_cols The columns to use as features in the Random Forest regression model. These columns should not have missing values.
#' @return The Spark DataFrame with missing values imputed in the target column
#' @export
#' @examples
#' #TBD

impute_with_random_forest_regressor <- function(sc, sdf, target_col, feature_cols) {
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

  # Step 2: Split the data into complete and incomplete rows
  # Reminder: all non target columns will have been initialized
  complete_data <- sdf %>%
    dplyr::filter(!is.na(!!rlang::sym(target_col)))

  incomplete_data <- sdf %>%
    dplyr::filter(is.na(!!rlang::sym(target_col)))

  # Step 3: Build regression formula
  formula_str <- paste0(target_col, " ~ ", paste(feature_cols, collapse = " + "))
  formula_obj <- stats::as.formula(formula_str)

  # Step 4: Build logistic regression model on complete data
  model <- complete_data %>%
    sparklyr::ml_random_forest_regressor(formula = formula_obj)

  # Step 5: Predict missing values
  predictions <- sparklyr::ml_predict(model, incomplete_data)

  # removing unused created columns (only need prediction)
  pre_pred_cols <- c(colnames(incomplete_data),"prediction")
  post_pred_cols <- colnames(predictions)
  extra_cols <- setdiff(post_pred_cols, pre_pred_cols)
  predictions <- predictions %>% dplyr::select(-dplyr::all_of(extra_cols))

  #print(predictions %>% select(prediction))

  # Replace the NULL values with predictions
  incomplete_data <- predictions %>%
    dplyr::select(-!!rlang::sym(target_col)) %>%  # Remove the original NULL column
    #dplyr::mutate(prediction = as.logical(prediction)) %>%
    dplyr::rename(!!rlang::sym(target_col) := prediction)  # Rename prediction to target_col

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

  # Step 3: Build regression formula
  formula_str <- paste0(target_col, " ~ ", paste(feature_cols, collapse = " + "))
  formula_obj <- stats::as.formula(formula_str)

  # Step 4: Build logistic regression model on complete data
  model <- complete_data %>%
    sparklyr::ml_random_forest_classifier(formula = formula_obj)

  # Step 5: Predict missing values
  predictions <- sparklyr::ml_predict(model, incomplete_data)

  # removing unused created columns (only need prediction)
  pre_pred_cols <- c(colnames(incomplete_data),"prediction")
  post_pred_cols <- colnames(predictions)
  extra_cols <- setdiff(post_pred_cols, pre_pred_cols)
  predictions <- predictions %>% dplyr::select(-dplyr::all_of(extra_cols))

  # Replace the NULL values with predictions
  incomplete_data <- predictions %>%
    dplyr::select(-!!rlang::sym(target_col)) %>%  # Remove the original NULL column
    #dplyr::mutate(prediction = as.logical(prediction)) %>%
    dplyr::rename(!!rlang::sym(target_col) := prediction)  # Rename prediction to target_col

  # Step 6: Combine complete and imputed data
  result <- complete_data %>%
    dplyr::union_all(incomplete_data)

  result <- result %>%
    dplyr::arrange(id) %>%
    dplyr::select(-id)

  return(result)
}
