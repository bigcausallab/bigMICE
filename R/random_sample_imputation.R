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
      dplyr::select(dplyr::all_of(c(col, "temp_row_id"))) %>%
      dplyr::filter(!is.na(!!rlang::sym(col)))

    missing_data <- sdf %>%
      dplyr::select(dplyr::all_of(c(col, "temp_row_id"))) %>%
      dplyr::filter(is.na(!!rlang::sym(col)))

    # Calculate sampling fraction
    n_missing <- sparklyr::sdf_nrow(missing_data)
    n_observed <- sparklyr::sdf_nrow(observed_data)

    fraction_missing <- n_missing / (n_missing + n_observed)

    # Skip if no missing or no observed values
    if (n_missing == 0 || n_observed == 0) {
      cat("No missing values or no observed values to sample from")
      next
    }
    cat("Sampling", n_missing, "values\n")

    #start_time <- Sys.time()
    frac_boosted <- n_missing/n_observed + 5/100
    #cat("Boosted fraction", frac_boosted, "\n")
    sampled_values <- observed_data %>%
      dplyr::select(!!rlang::sym(col)) %>%
      sparklyr::sdf_sample(fraction = frac_boosted, replacement = TRUE) %>%
      utils::head(n_missing) %>%
      sparklyr::sdf_with_sequential_id(id = "id")
    #end_time <- Sys.time()
    #cat("Time taken to sample values4:", end_time - start_time, "\n")
    #n_sampled4 <- sparklyr::sdf_nrow(sampled_values)
    #cat(" n_sampled4", n_sampled4,"\n")

    print(colnames(sampled_values))
    print(sampled_values %>% head(5))

    # Add sequential ID to missing_data for joining
    missing_data_with_id <- missing_data %>% sparklyr::sdf_with_sequential_id(id = "id")

    # Replace NA values with sampled values
    imputed_data <- missing_data_with_id %>%
      dplyr::left_join(sampled_values %>% dplyr::rename(value_new = !!rlang::sym(col)), by = "id") %>%
      dplyr::mutate(!!rlang::sym(col) := dplyr::coalesce(value_new, !!rlang::sym(col))) %>%
      dplyr::select(-id, -value_new)

    # Union with observed data and sort by temp_row_id
    new_col_data <- imputed_data %>%
      dplyr::union(observed_data) %>%
      dplyr::arrange(temp_row_id)

    # Update the column in sdf_with_id
    # Since new_col_data is a Spark DataFrame, we join and replace
    sdf <- sdf %>% dplyr::select(-!!rlang::sym(col)) %>%  # Drop old column
      dplyr::left_join(new_col_data %>% dplyr::select(temp_row_id, !!rlang::sym(col)), by = "temp_row_id")

  } #End of for loop over columns
  # Remove the temporary ID column and return
  sdf %>% dplyr::arrange(temp_row_id) %>% dplyr::select(-"temp_row_id")
  return(sdf)
}


#' Random Sample Imputation function
#'
#' This function imputes missing values in a Spark DataFrame using random samples from the observed values.
#' @importFrom dplyr %>%
#'
#' @param sc A Spark connection
#' @param sdf A Spark DataFrame
#' @param column The column(s) to impute. If NULL, all columns will be imputed
#' @param checkpointing Default TRUE. Can be set to FALSE if you are running the package without access to a HDFS directory for checkpointing. It is strongly recommended to keep it to TRUE to avoid Stackoverflow errors.
#' @return The Spark DataFrame with missing values imputed
#' @export
#' @examples
#' #TBD
init_with_random_samples<- function(sc, sdf, column = NULL, checkpointing = TRUE) {

  cols_to_process <- if (!is.null(column)) column else colnames(sdf)
  # Add sequential ID to preserve original order
  sdf <- sdf %>% sparklyr::sdf_with_sequential_id(id = "temp_row_id")
  num_cols <- length(cols_to_process)
  i <- 0
  # Process each specified column
  for (col in cols_to_process) {
    i <- i + 1
    cat("\n", i, "/", num_cols)

    cat(":", col, "- ")

    # Skip if column doesn't exist
    if (!(col %in% colnames(sdf))) {
      warning(paste("Column", col, "not found in dataframe. Skipping."))
      next
    }

    # Separate observed and missing values while maintaining original order
    observed_data <- sdf %>% dplyr::filter(!is.na(!!rlang::sym(col)))

    missing_data <- sdf %>% dplyr::filter(is.na(!!rlang::sym(col))) %>%
      sparklyr::sdf_with_sequential_id(id = "id")

    n_missing <- sparklyr::sdf_nrow(missing_data)
    n_observed <- sparklyr::sdf_nrow(observed_data)

    fraction_missing <- n_missing / (n_missing + n_observed)

    if (n_missing == 0 || n_observed == 0) {
      cat("No missing values or no observed values to sample from")
      next
    }
    cat("Sampling", n_missing, "values")

    # sdf_sample is fast but not precise 100% of the time so I oversample, then truncate.
    # 5% extra is enough to work most of the time
    frac_boosted <- n_missing/n_observed + 5/100
    sampled_values <- observed_data %>%
      dplyr::select(!!rlang::sym(col)) %>%
      sparklyr::sdf_sample(fraction = frac_boosted, replacement = TRUE) %>%
      utils::head(n_missing) %>%
      sparklyr::sdf_with_sequential_id()

    n_sampled_values <- sparklyr::sdf_nrow(sampled_values)

    # While oversampling works most of the time, it still sometimes misses, so I resample until it is accurate
    while(n_sampled_values != n_missing){
      cat(" sampling failed, resampling...\n")
      sampled_values <- observed_data %>%
        dplyr::select(!!rlang::sym(col)) %>%
        sparklyr::sdf_sample(fraction = frac_boosted, replacement = TRUE) %>%
        utils::head(n_missing) %>%
        sparklyr::sdf_with_sequential_id()

      n_sampled_values <- sparklyr::sdf_nrow(sampled_values)
    }

    # Replace NA values with sampled values
    imputed_data <- missing_data %>%
      dplyr::left_join(sampled_values %>% dplyr::rename(value_new = !!rlang::sym(col)), by = "id") %>%
      dplyr::mutate(!!rlang::sym(col) := dplyr::coalesce(value_new, !!rlang::sym(col))) %>%
      dplyr::select(-id, -value_new)

    new_col_data <- imputed_data %>% dplyr::union(observed_data)

    sdf <- sdf %>%
      dplyr::select(-!!rlang::sym(col)) %>%  # Drop old column
      dplyr::left_join(new_col_data %>%
      dplyr::select(temp_row_id, !!rlang::sym(col)), by = "temp_row_id")

    if(checkpointing & i%%10 == 0){
      sdf <- sparklyr::sdf_checkpoint(sdf, eager=TRUE)
    }

  } #End of for loop over columns

  sdf %>% dplyr::arrange(temp_row_id) %>% dplyr::select(-"temp_row_id")
  return(sdf)
}
