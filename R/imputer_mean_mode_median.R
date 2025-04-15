#' Mean/Mode/Median Imputation function
#'
#' This function imputes missing values in a Spark DataFrame using the mean, mode, or median of the observed values.
#' @importFrom dplyr %>%
#'
#'
#' @param sc A Spark connection
#' @param sdf A Spark DataFrame
#' @param column The column(s) to impute. If NULL, all columns will be imputed
#' @param impute_mode Which imputation method to use for each column. Options are "mean", "mode", "median", or "none"
#' @param printFlags Wether or not to print the imputation process to the console. Default is TRUE.
#' @return The Spark DataFrame with missing values imputed
#' @export
#' @examples
#' #TBD

impute_with_MeMoMe  <- function(sc, sdf, column = NULL, impute_mode, printFlags = TRUE) {
  # Validate impute_mode is provided
  if (missing(impute_mode)) {
    stop("impute_mode is a mandatory argument and must be specified for each column")
  }

  # Determine which columns to process
  cols_to_process <- if (!is.null(column)) {
    if (!is.character(column) || length(column) == 0) {
      stop("column must be a character string")
    }
    column
  } else {
    colnames(sdf)
  }

  # Validate impute_mode matches number of columns
  if (length(impute_mode) != length(cols_to_process)) {
    stop("Length of impute_mode must match number of columns being processed")
  }

  # Validate impute_mode values
  valid_modes <- c("mean", "mode", "median", "none")
  invalid_modes <- setdiff(impute_mode, valid_modes)
  if (length(invalid_modes) > 0) {
    stop(paste("Invalid imputation modes:", paste(invalid_modes, collapse=", "),
               "\nValid modes are:", paste(valid_modes, collapse=", ")))
  }

  # Add a sequential ID to preserve row order
  result <- sdf %>%
    sparklyr::sdf_with_sequential_id()

  num_cols <- length(cols_to_process)
  # Process each column
  for (i in seq_along(cols_to_process)) {
    col <- cols_to_process[i]
    mode <- impute_mode[i]
    # Print progress
    if (printFlags) {
      cat("\n",i,"/",num_cols,":", col, "- ")
    }

    # Skip columns that don't exist
    if (!(col %in% colnames(result))) {
      warning(paste("Column", col, "not found in dataframe. Skipping."))
      next
    }

    # If mode is "none", skip to next column
    if (mode == "none") {
      if (printFlags) {
        cat(" Skipping imputation as specified.")
      }
      next
    }

    # Split into complete and incomplete data for this column
    complete_data <- result %>%
      dplyr::filter(!is.na(!!rlang::sym(col)))

    incomplete_data <- result %>%
      dplyr::filter(is.na(!!rlang::sym(col)))

    # If no missing values or no observed values, skip this column
    if (sparklyr::sdf_nrow(incomplete_data) == 0) {
      if (printFlags) {
        cat(" No missing values found. Skipping.")
      }
      next
    }

    if (sparklyr::sdf_nrow(complete_data) == 0) {
      if (printFlags) {
        cat(" No observed values found. Skipping.")
      }
      next
    }

    # Calculate imputation value based on mode
    impute_value <- if (mode == "mean") {
      if (printFlags) {
        cat("Imputing with mean")
      }
      value <- complete_data %>%
        dplyr::summarize(impute_val = mean(!!rlang::sym(col), na.rm = TRUE)) %>%
        dplyr::collect() %>%
        dplyr::pull(impute_val)
      if (printFlags) {
        cat(" - Mean value:", value)
      }
      value
    } else if (mode == "median") {
      if (printFlags) {
        cat("Imputing with median")
      }
      value <- complete_data %>%
        dplyr::summarize(impute_val = stats::median(!!rlang::sym(col), na.rm = TRUE)) %>%
        dplyr::collect() %>%
        dplyr::pull(impute_val)
      if (printFlags) {
        cat(" - Median value:", value)
      }
      value
    } else if (mode == "mode") {
      if (printFlags) {
        cat("Imputing with mode")
      }
      value <- complete_data %>%
        dplyr::group_by(!!rlang::sym(col)) %>%
        dplyr::summarize(count = dplyr::n()) %>%
        dplyr::arrange(dplyr::desc(count)) %>%
        dplyr::collect() %>%
        utils::head(1) %>%
        dplyr::pull(!!rlang::sym(col))
      if (printFlags) {
        cat(" - Mode value:", value)
      }
      value
    }

    # Create imputed dataframe
    imputed_df <- result %>%
      dplyr::mutate(!!rlang::sym(col) := dplyr::if_else(is.na(!!rlang::sym(col)), impute_value, !!rlang::sym(col)))

    # Update result with imputed dataframe
    result <- imputed_df
  }

  # Sort by the sequential ID and drop the ID column to restore original row order
  result <- result %>%
    dplyr::arrange(id) %>%
    dplyr::select(-id)

  return(result)
}
