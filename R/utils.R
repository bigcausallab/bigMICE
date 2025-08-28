#' Get Named vector
#'
#' Utility function to read a CSV file containing Variable Name and Variable Type columns and return a named vector of the variable types with the variable names as names. The vector is ordered alphabetically by variable name.
#'
#' @param csv_file Path to the CSV file
#' @return A named vector of the variable types with the variable names as names, ordered alphabetically by variable name.
#' @export
#' @examples
#' #TBD
get_named_vector <- function(csv_file) {
  # Helper function to extract the named datatypes of a dataset.
  data <- utils::read.csv(csv_file, stringsAsFactors = FALSE)
  named_vector <- stats::setNames(data$Variable.type, data$Variable.Name)
  ordered_vector <- named_vector[order(names(named_vector))] #Order alphabet.
  return(ordered_vector)
}

#' Create Predictor Matrix csv
#'
#' Utility function to create a matrix or a csv File containing the variables names x variables names matrix to be used for specifying the predictor matrix to be used during the bigMice procedure. The csv file can then be opened with a spreadsheet software (e.g. Excel, Google sheets, etc.) to visualize and specify the predictors of each variable more easily and in a friendlier interface.
#'
#' @param df The dataframe, including the column names.
#' @param save_file Default value is NULL. If a save file is specified, the function will save the created matrix as a .csv file with the name and location specified.
#' @return A predictor matrix with default settings (all predictors used) for the bigMice imputation procedure.
#' @export
#' @examples
#' #TBD
create_predictor_matrix <- function(df, save_file=NULL){
  # Utility function to create a predictor matrix for bigMICE
  # takes a R dataframe and saves a excel file with the default predictor matrix
  # optional argument save to also save the matrix to an csv file to be modified (easier)

  # Extract variable names
  variables <- colnames(df)

  # Create a TRUE matrix with FALSE on the diagonal
  predictor_matrix <- matrix(TRUE, nrow = length(variables), ncol = length(variables),
                             dimnames = list(variables, variables))
  diag(predictor_matrix) <- FALSE  # Set diagonal to FALSE

  # save the matrix to an excel file
  if (save){
    utils::write.csv(as.data.frame(predictor_matrix), "predictor_matrix.csv")
  }
  return(predictor_matrix)
}


#' Import Predictor Matrix csv
#'
#' Utility function to import back a predictor matrix from a csv file created by the create_predictor_matrix() function.
#'
#' @param file The csv file to be opened
#' @return A predictor matrix with specified settings (predictors used) for the bigMice imputation procedure.
#' @export
#' @examples
#' #TBD
#'
import_predictor_matrix <- function(file){
  # Utility function to import a predictor matrix from an csv file
  # takes a file path and returns the matrix as a matrix object
  data <- utils::read.csv(file, row.names = 1)
  return(as.matrix(data))
}

#' Print Method for Multiple Imputation Results
#'
#' @param x A list containing multiple imputation results with Rubin's statistics
#' @param digits Number of digits to display for numeric values (default: 4)
#' @param show_individual Logical, whether to show individual imputation results (default: FALSE)
#' @param ... Additional arguments (not currently used)
#' @export
print.mi_results <- function(x, digits = 4, show_individual = FALSE, ...) {

  # Validate input
  if (!is.list(x) || !all(c("rubin_stats", "per_imputation") %in% names(x))) {
    stop("Input must be a list with 'rubin_stats' and 'per_imputation' components")
  }

  # Header
  cat("Multiple Imputation Results\n")
  cat("==========================\n\n")

  # Summary information
  m <- nrow(x$per_imputation)
  cat("Number of imputations:", m, "\n")

  if ("imputation_time" %in% names(x$per_imputation)) {
    total_time <- sum(x$per_imputation$imputation_time, na.rm = TRUE)
    avg_time <- mean(x$per_imputation$imputation_time, na.rm = TRUE)
    cat("Total imputation time:", round(total_time, 2), "seconds\n")
    cat("Average time per imputation:", round(avg_time, 2), "seconds\n")
  }

  cat("\n")

  # Pooled Results using Rubin's Rules
  cat("Pooled Parameter Estimates (Rubin's Rules)\n")
  cat("==========================================\n")

  if (length(x$rubin_stats) > 0) {
    # Create summary table
    param_names <- names(x$rubin_stats)
    n_params <- length(param_names)

    # Initialize result matrix
    result_table <- matrix(NA, nrow = n_params, ncol = 6)
    colnames(result_table) <- c("Estimate", "Within_Var", "Between_Var",
                                "Total_Var", "SE", "t_stat")
    rownames(result_table) <- param_names

    for (i in seq_along(param_names)) {
      param <- param_names[i]
      stats <- x$rubin_stats[[param]]

      result_table[i, "Estimate"] <- stats$pooled_param
      result_table[i, "Within_Var"] <- stats$within_var
      result_table[i, "Between_Var"] <- stats$between_var
      result_table[i, "Total_Var"] <- stats$total_var
      result_table[i, "SE"] <- sqrt(stats$total_var)

      # t-statistic (assuming null hypothesis: parameter = 0)
      if (stats$total_var > 0) {
        result_table[i, "t_stat"] <- stats$pooled_param / sqrt(stats$total_var)
      }
    }

    # Print the table with specified digits
    print(round(result_table, digits))

    # Additional diagnostics
    cat("\nDiagnostic Information:\n")
    cat("-----------------------\n")
    for (param in param_names) {
      stats <- x$rubin_stats[[param]]
      if (!is.null(stats$values) && length(stats$values) > 1) {
        # Relative increase in variance due to nonresponse
        r <- (stats$between_var + stats$between_var/m) / stats$within_var
        # Fraction of missing information
        lambda <- (stats$between_var + stats$between_var/m) / stats$total_var
        # Degrees of freedom
        df <- (m - 1) * (1 + 1/r)^2

        cat(sprintf("%-15s: r=%.3f, lambda=%.3f, df=%.1f\n",
                    param, r, lambda, df))
      }
    }
  } else {
    cat("No pooled statistics available.\n")
  }

  cat("\n")

  # Individual imputation results (optional)
  if (show_individual && nrow(x$per_imputation) > 0) {
    cat("Individual Imputation Results\n")
    cat("=============================\n")

    # Select relevant columns for display
    display_cols <- names(x$per_imputation)
    # Remove non-numeric columns for cleaner display
    numeric_cols <- sapply(x$per_imputation, is.numeric)

    if (any(numeric_cols)) {
      per_imp_display <- x$per_imputation[, numeric_cols, drop = FALSE]
      per_imp_display[, numeric_cols] <- lapply(per_imp_display[, numeric_cols],
                                                function(x) round(x, digits))
      print(per_imp_display)
    } else {
      print(x$per_imputation)
    }
    cat("\n")
  }

  # Footer with usage notes
  cat("Notes:\n")
  cat("------\n")
  cat("- SE: Standard Error (sqrt of Total_Var)\n")
  cat("- t_stat: t-statistic for testing parameter = 0\n")
  cat("- r: Relative increase in variance due to nonresponse\n")
  cat("- lambda: Fraction of missing information\n")
  cat("- df: Degrees of freedom for t-distribution\n")
  if (!show_individual) {
    cat("- Use show_individual=TRUE to see results from each imputation\n")
  }

  invisible(x)
}

#' Summary Method for Multiple Imputation Results
#'
#' @param object A list containing multiple imputation results
#' @param ... Additional arguments passed to print method
#' @export
summary.mi_results <- function(object, ...) {
  print(object, ...)
}

# Example usage function to set the class
#' Create MI Results Object
#'
#' @param results_list List containing rubin_stats and per_imputation components
#' @return An object of class 'mi_results'
#' @export
create_mi_results <- function(results_list) {
  class(results_list) <- c("mi_results", "list")
  return(results_list)
}
