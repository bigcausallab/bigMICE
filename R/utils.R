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
    write.csv(as.data.frame(predictor_matrix), "predictor_matrix.csv")
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
  data <- read.csv(file, row.names = 1)
  return(as.matrix(data))
}
