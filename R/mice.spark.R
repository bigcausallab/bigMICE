#' MICE for Spark DataFrames using Sparklyr and Spark MLlib
#'
#' This function imputes missing values in a Spark DataFrame using MICE (Multiple Imputation by Chained Equations) algorithm.
#'
#' @param sc A Spark connection
#' @param data A Spark DataFrame
#' @param variable_types A named character vector, the variable types of the columns in the data.
#' @param analysis_formula A formula, the formula to use for the analysis
#' @param m The number of imputations to perform
#' @param method A character vector, the imputation method to use for each variable. If NULL, the function will infer the method based on the variable types.
#' @param predictorMatrix A matrix, the predictor matrix to use for the imputation. TBD
#' @param formulas A list, the formulas to use for the imputation. If NULL, the function will infer the formulas based on the other variables present in the data. TBD
#' @param modeltype A character vector, the model type to use for the imputation. If NULL, the function will infer the model type based on the variable types. TBD
#' @param maxit The maximum number of iterations to perform
#' @param printFlag A boolean, whether to print debug information
#' @param seed An integer, the seed to use for reproducibility
#' @param imp_init A Spark DataFrame, the original data with missing values, but with initial imputation (by random sampling or mean/median/mode imputation). Can be set to avoid re-running the initialisation step. Otherwise, the function will perform the initialisation step using the MeMoMe function.
#' @param ... Additional arguments to be passed to the function. TBD
#' @return A list containing the Rubin's statistics for the model parameters, the per-imputation statistics, the imputation statistics, and the model parameters.
#' @export
#' @examples
#' #TBD

mice.spark <- function(data,
                       sc,
                       variable_types, # Used for initialization and method selection
                       analysis_formula,
                       m = 5,
                       method = NULL,
                       predictorMatrix = NULL,
                       formulas = NULL,
                       modeltype = NULL,
                       maxit = 5,
                       printFlag = TRUE,
                       seed = NA,
                       imp_init = NULL,
                       ...) {


  if (!is.na(seed)) set.seed(seed)

  # check form of data and m
  #data <- check.spark.dataform(data)
  cols <- names(variable_types)
  #m <- check.m(m)


  from <- 1
  to <- from + maxit - 1

  # INITIALISE THE IMPUTATION USING Mean/Mode/Median SAMPLING

  # Do this inside or outside the m loop ?
  # Do I want each imputation to start from the same sample or have more variation in initial condition ?

  #TODO : add support for column parameter in initialisation

  # Dictionnary to infer initialization method based on variable type
  # Should be one of (mean, mode, median, none), and be used as input of MeMoMe function
  init_dict <- c("Binary" = "mode",
                 "Nominal" = "mode",
                 "Ordinal" = "mode",
                 "Code (don't impute)" = "none", # LopNr, Unit_code etc...
                 "Continuous_int" = "median",
                 "Continuous_float" = "mean",
                 "smalldatetime" = "none",
                 "String" = "none", #TBD
                 "Count" = "median", #TBD
                 "Semi-continuous" = "none", #TBD
                 "Else", "none")

  init_modes <- replace(variable_types, variable_types %in% names(init_dict), init_dict[variable_types])
  names(init_modes) <- cols
  # print("**DEBUG**: init_modes:")
  # print(init_modes)

  cat("\nStarting initialisation\n")
  # print(" ")

  # print(length(init_modes))
  # print(sdf_ncol(data))
  #
  init_start_time <- proc.time()
  if(is.null(imp_init)){
    imp_init <- impute_with_MeMoMe(sc = sc,
                                   sdf = data,
                                   column = NULL, #TODO: add support for this
                                   impute_mode = init_modes)
  }else{
    print("Using initial imputation provided manually, I hope it is correct")
    imp_init <- imp_init # User provided initiale imputation
  }

  init_end_time <- proc.time()
  init_elapsed <- (init_end_time-init_start_time)['elapsed']
  cat("Initalisation time:", init_elapsed)
  # TODO : Add elapse time to the result dataframe (and create result dataframe)

  ### Rubin Rules Stats INIT###
  # Get the formula for the model
  formula_obj <- analysis_formula
  param_names <- c("(Intercept)", all.vars(formula_obj)[-1])

  model_params <- vector("list", m)

  # List to store per-imputation information
  imputation_stats <- vector("list", m)
  #############################


  # FOR EACH IMPUTATION SET i = 1, ..., m
  for (i in 1:m) {
    cat("Iteration: ", i, "\n")

    # Run the imputation algorithm
    cat("Starting imputation")

    imp_start_time <- proc.time()

    imp <- sampler.spark(sc = sc,
                         data = data,
                         imp_init = imp_init,
                         fromto = c(from, to),
                         var_types = variable_types,
                         printFlag = printFlag)

    imp_end_time <- proc.time()
    imp_elapsed <- (imp_end_time-imp_start_time)['elapsed']
    cat("\nImputation time:", imp_elapsed,".\n")

    # Save imputation to dataframe ? Maybe only the last one ?

    # Compute user-provided analysis on the fly on the imputed data ?

    # ##### Calculate Rubin Rules statistics ############
    # Fit model on imputed data
    cat("Fitting model on imputed data\n")
    model <- imp %>%
      ml_logistic_regression(formula = formula_obj)

    # Store model coefficients
    model_params[[i]] <- model$coefficients
    print(model$coefficients)
    # Create per-imputation summary for this iteration
    imp_summary <- list(
      imputation_number = i,
      imputation_time = imp_elapsed
    )

    # Add model coefficients to the imputation summary
    for (param in param_names) {
      if (param %in% names(model$coefficients)) {
        imp_summary[[param]] <- model$coefficients[[param]]
      } else {
        # Handle case where parameter might not be in the model
        imp_summary[[param]] <- NA
      }
    }

    # Save this imputation's stats
    imputation_stats[[i]] <- imp_summary

  } # END FOR EACH IMPUTATION SET i = 1, ..., m

  # Rubin's Statistics for model parameters
  results <- list()

  # Create a matrix of parameters from all imputations
  params_matrix <- do.call(rbind, model_params)

  for (param in param_names) {
    if (param %in% colnames(params_matrix)) {
      param_values <- params_matrix[, param]

      # Calculate Rubin's statistics
      pooled_param <- mean(param_values, na.rm = TRUE)
      between_var <- sum((param_values - pooled_param)^2) / (m - 1)

      # For model parameters, within variance needs to be estimated from model
      # Here we'll use a simplified approach - using the variance of the estimates
      # In a more complete implementation, this would come from the model's variance-covariance matrix
      within_var <- mean((param_values - pooled_param)^2) / m

      total_var <- within_var + between_var + (between_var / m)

      results[[param]] <- list(
        pooled_param = pooled_param,
        within_var = within_var,
        between_var = between_var,
        total_var = total_var,
        values = param_values
      )
    }
  }

  # data frame for per-imputation statistics
  per_imputation_df <- do.call(rbind, lapply(imputation_stats, function(imp) {
    data.frame(imp, stringsAsFactors = FALSE)
  }))

  # Returning both the aggregated results and per-imputation statistics
  return(list(
    rubin_stats = results,
    per_imputation = per_imputation_df,
    imputation_stats = imputation_stats,
    model_params = model_params
  ))
}


#' MICE sampler function
#'
#' This function is the core of the MICE algorithm. It iteratively imputes missing values in a Spark DataFrame using a set of imputation methods based on the variable types.
#'
#' @param sc A Spark connection
#' @param data A Spark DataFrame, the original data with missing values
#' @param imp_init A Spark DataFrame, the original data with missing values, but with initial imputation (by random sampling or mean/median/mode imputation)
#' @param fromto A vector of length 2, the range of iterations to perform (from, to)
#' @param var_types A named character vector, the variable types of the columns in the data.
#' @param printFlag A boolean, whether to print debug information.
#' @return The Spark DataFrame with missing values imputed for all variables
#' @export
#' @examples
#' #TBD
sampler.spark <- function(sc,
                          data,
                          imp_init,
                          fromto,
                          var_types,
                          printFlag){


  #TODO; add support for functionalities present in the mice() function (where, ignore, blocks, predictorMatrix, formula, ...)

  # For iteration k in fromto
  from = fromto[1]
  to = fromto[2]

  var_names <- names(sparklyr::sdf_schema(data))

  # Method dictionary for imputation. Can change as desired
  # TODO: implement; keep this as default, or use user-provided dict ?
  method_dict <- c("Binary" = "Logistic",
                   "Nominal" = "Mult_Logistic",
                   "Ordinal" = "RandomForestClassifier",
                   "Code (don't impute)" = "none", # LopNr, Unit_code etc...
                   "Continuous_int" = "Linear",
                   "Continuous_float" = "Linear",
                   "smalldatetime" = "none",  #TBD
                   "String" = "none", #TBD
                   "Count" = "RandomForestClassifier", #TBD
                   "Semi-continuous" = "none", #TBD
                   "Else" = "none")

  imp_methods <- replace(var_types, var_types %in% names(method_dict), method_dict[var_types])
  names(imp_methods) <- var_names
  print(imp_methods)
  num_vars <- length(var_names)

  for (k in from:to){
    cat("\n iteration: ", k)
    # For each variable j in the data
    j <- 0
    for (var_j in var_names){
      j <- j+1
      cat("\n",j,"/",num_vars,"Imputing variable", var_j," using method ")
      # Obtain the variables use to predict the missing values of variable j and create feature column
      label_col <- var_j

      feature_cols <- setdiff(var_names, label_col)
      #print(feature_cols)
      #Filter out Date data type
      feature_cols <- feature_cols[sapply(var_types[feature_cols],
                                          function(x) !(x %in% c("String", "smalldatetime")))]

      # Replace initialized values in label column with the original missing values
      j_df <- imp_init %>%
        sparklyr::select(-label_col) %>%
        cbind(data %>% sparklyr::select(all_of(label_col)))


      method <- imp_methods[[var_j]]
      cat(method)

      result <- switch(method,
                       "Logistic" = impute_with_logistic_regression(sc, j_df, label_col, feature_cols),
                       "Mult_Logistic" = impute_with_mult_logistic_regression(sc, j_df, label_col, feature_cols),
                       "Linear" = impute_with_linear_regression(sc, j_df, label_col, feature_cols),
                       "RandomForestClassifier" = impute_with_random_forest_classifier(sc, j_df, label_col, feature_cols),
                       "none" = j_df, # don't impute this variable
                       "Invalid method"  # Default case
      )
      #Use the result to do something to the original dataset
      #print(result)
      # To avoid stackoverflow error, I try to break/collect? the lineage after each imputation
      # Might not be necessary at every iteration. only run into error after ~25 imputation
      result %>% sdf_persist()
      # But this does not seems to work. Still run into stack_overflow error
    } #end of var_j loop (each variable)

  } #end of k loop (iterations)
  # The sampler has finish his iterative work, can now return the imputed dataset ?
  return(result)
}
