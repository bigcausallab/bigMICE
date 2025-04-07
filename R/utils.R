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
