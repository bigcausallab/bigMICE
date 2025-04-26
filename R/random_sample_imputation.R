#' Random Sample Imputation function
#'
#' This function imputes missing values in a Spark DataFrame using random samples from the observed values.
#' @importFrom dplyr %>%
#'
#'
#' @param sc A Spark connection
#' @param sdf A Spark DataFrame
#' @param column The column(s) to impute. If NULL, all columns will be imputed
#' @return The Spark DataFrame with missing values imputed
#' @export
#' @examples
#' #TBD
impute_with_random_samples<- function(sc, sdf, column = NULL) {

  # Determine columns to process
  cols_to_process <- if (!is.null(column)) column else colnames(sdf)

  # Add sequential ID to preserve original order
  sdf <- sdf %>% sparklyr::sdf_with_sequential_id(id = "temp_row_id")

  num_cols <- length(cols_to_process)
  i <- 0

  # Process each specified column
  for (col in cols_to_process) {
    cat("\nVariable", i, "out of", num_cols)
    i <- i + 1
    cat(":", col, "- ")
    print(" ")
    # Skip if column doesn't exist
    if (!(col %in% colnames(sdf))) {
      warning(paste("Column", col, "not found in dataframe. Skipping."))
      next
    }

    # Separate observed and missing values while maintaining original order
    observed_data <- sdf %>%
      dplyr::select(all_of(c(col, "temp_row_id"))) %>%
      dplyr::filter(!is.na(!!rlang::sym(col)))

    missing_data <- sdf %>%
      dplyr::select(all_of(c(col, "temp_row_id"))) %>%
      dplyr::filter(is.na(!!rlang::sym(col)))

    # Calculate sampling fraction
    n_missing <- sparklyr::sdf_nrow(missing_data)
    n_observed <- sparklyr::sdf_nrow(observed_data)

    print("Fraction of missing values:")
    fraction_missing <- n_missing / (n_missing + n_observed)
    print(fraction_missing)

    # Skip if no missing or no observed values
    if (n_missing == 0 || n_observed == 0) {
      cat("No missing values or no observed values to sample from")
      next
    }
    cat("Sampling", n_missing, "values")

    # Sample n_missing values from the observed values
    sampled_values <- observed_data %>%
      dplyr::select(!!rlang::sym(col)) %>%  # Only need the column to sample
      dplyr::sample_n(size = n_missing, replace = TRUE) %>%
      sparklyr::sdf_with_sequential_id(id = "id")

    n_sampled <- sparklyr::sdf_nrow(sampled_values)
    cat(" n_sampled", n_sampled,"\n")

    sampled_values2 <- observed_data %>%
      dplyr::select(!!rlang::sym(col)) %>%
      sparklyr::sdf_sample(fraction = fraction_missing, replacement = TRUE) %>%
      dplyr::head(n_missing) %>%
      sparklyr::sdf_with_sequential_id(id = "id")

    n_sampled2 <- sparklyr::sdf_nrow(sampled_values2)
    cat(" n_sampled2", n_sampled2,"\n")

    # Add sequential ID to missing_data for joining
    # missing_data_with_id <- missing_data %>%
    #   sdf_with_sequential_id(id = "id")

    # Replace NA values with sampled values
    # imputed_data <- missing_data_with_id %>%
    #   left_join(sampled_values %>% rename(value_new = !!rlang::sym(col)), by = "id") %>%
    #   mutate(!!rlang::sym(col) := coalesce(value_new, !!rlang::sym(col))) %>%
    #   select(-id, -value_new)

    # Union with observed data and sort by temp_row_id
    # new_col_data <- imputed_data %>%
    #   dplyr::union(observed_data) %>%
    #   dplyr::arrange(temp_row_id)

    # Update the column in sdf_with_id
    # Since new_col_data is a Spark DataFrame, we join and replace
    # sdf_with_id <- sdf_with_id %>%
    #   select(-!!rlang::sym(col)) %>%  # Drop old column
    #   left_join(new_col_data %>% select(temp_row_id, !!rlang::sym(col)), by = "temp_row_id")
    #
  }

  # Remove the temporary ID column and return
  sdf %>% dplyr::arrange(temp_row_id) %>% dplyr::select(-"temp_row_id")
}

