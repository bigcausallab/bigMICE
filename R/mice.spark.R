#' MICE for Spark DataFrames using Sparklyr and Spark MLlib
#'
#' This function imputes missing values in a Spark DataFrame using MICE (Multiple Imputation by Chained Equations) algorithm.
#'
#' @importFrom dplyr %>%
#'
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
#' @param checkpointing Default TRUE. Can be set to FALSE if you are running the package without access to a HDFS directory for checkpointing. It is strongly recommended to keep it to TRUE to avoid Stackoverflow errors.
#' @param checkpoint_frequency Advanced parameter, modify with care. If checkpointing = TRUE, how often to checkpoint , default = 10, so after processing every 10 variables, the lineage will be cut and the current state of computation will be save to disk. A low number might slow down computation but enable bigger computation. A number too high (or not checkpoiting) might cause JVM stackOverflowError as the lineage will have grown too big.
#' @param ... Additional arguments to be passed to the function. TBD
#' @return A list containing the Rubin's statistics for the model parameters, the per-imputation statistics, the imputation statistics, and the model parameters.
#' @export
#' @examples
#' # Example for mice.spark function
#' #library(sparklyr)
#' #library(dplyr)
#'
#' # Connect to Spark
#' #sc <- spark_connect(master = "local")
#'
#' # Create sample data with missing values
#' #sample_data <- data.frame(
#'  # outcome = c(1, 0, NA, 1, NA, 0),
#'  # age = c(25, NA, 35, 28, 45, NA),
#'  # income = c(50000, 60000, NA, 55000, 80000, 52000),
#'  # education = c("High", "Medium", "High", NA, "Low", "Medium")
#' #)
#'
#' # Copy to Spark DataFrame
#' #sdf <- copy_to(sc, sample_data, "sample_data")
#'
#' # Define variable types
#' #variable_types <- c(
#'  # outcome = "Binary",
#'  # age = "Continuous_int",
#'  # income = "Continuous_int",
#'  # education = "Nominal"
#' #)
#'
#' # Define analysis formula
#' #analysis_formula <- outcome ~ age + income + education
#'
#' # Run MICE imputation
#' #mice_result <- mice.spark(
#'  # data = sdf,
#'  # sc = sc,
#'  # variable_types = variable_types,
#'  # analysis_formula = analysis_formula,
#'  # m = 3,  # Number of imputations
#'  # maxit = 2,  # Number of iterations
#'  # printFlag = TRUE,
#'  # seed = 123,
#'  # checkpointing = FALSE  # Set to TRUE if HDFS is available
#' #)
#'
#' # View Rubin's pooled statistics
#' #mice_result$rubin_stats

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
                       checkpointing = TRUE,
                       checkpoint_frequency = 10,
                       ...) {

  if (!is.na(seed)) set.seed(seed)

  from <- 1
  to <- from + maxit - 1

  cols <- names(variable_types)
  # Do this inside or outside the m loop ?
  # Do I want each imputation to start from the same sample or have more variation in initial condition ?

  #TODO : add support for column parameter in initialisation

  ### Rubin Rules Stats INIT###
  # Get the formula for the model
  formula_obj <- analysis_formula
  param_names <- c("(Intercept)", all.vars(formula_obj)[-1])

  model_params <- vector("list", m)

  # List to store per-imputation information
  imputation_stats <- vector("list", m)

  # FOR EACH IMPUTATION SET i = 1, ..., m
  for (i in 1:m) {
    cat("\nStarting initialisation\n")

    init_start_time <- proc.time()

    imp_init <- init_with_random_samples(sc, data, column = NULL, checkpointing = checkpointing)

    # Check that the initialised data does not contain any missing values
    init_end_time <- proc.time()
    init_elapsed <- (init_end_time-init_start_time)['elapsed']
    cat("\nInitalisation time:", init_elapsed)

    cat("\nImputation: ", i, "\n")

    imp_start_time <- proc.time()

    imp <- sampler.spark(sc = sc,
                         data = data,
                         imp_init = imp_init,
                         fromto = c(from, to),
                         var_types = variable_types,
                         predictorMatrix = predictorMatrix,
                         printFlag = printFlag,
                         checkpointing = checkpointing,
                         checkpoint_frequency = checkpoint_frequency)

    imp_end_time <- proc.time()
    imp_elapsed <- (imp_end_time-imp_start_time)['elapsed']
    cat("\nImputation time:", imp_elapsed,".\n")

    # Save imputation to dataframe ? Maybe only the last one ?

    # Compute user-provided analysis on the fly on the imputed data ?

    # Calculate Rubin Rules statistics
    # Fit model on imputed data
    cat("Fitting model on imputed data\n")

    model <- imp %>%
      sparklyr::ml_logistic_regression(formula = formula_obj)

    print(model)
    # Store model coefficients
    model_params[[i]] <- model$coefficients
    #print(model$coefficients)
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
#' @param ud_methods The user-defined methods for imputing each variables. Beta
#' @param printFlag A boolean, whether to print debug information.
#' @param predictorMatrix A matrix, the predictor matrix to use for the imputation. Beta
#' @param checkpointing Default TRUE. Can be set to FALSE if you are running the package without access to a HDFS directory for checkpointing. It is strongly recommended to keep it to TRUE to avoid Stackoverflow errors.
#' @param checkpoint_frequency Advanced parameter, modify with care. If checkpointing = TRUE, how often to checkpoint , default = 10, so after processing every 10 variables, the lineage will be cut and the current state of computation will be save to disk. A low number might slow down computation but enable bigger computation. A number too high (or not checkpoiting) might cause JVM stackOverflowError as the lineage will have grown too big.
#' @return The Spark DataFrame with missing values imputed for all variables
#' @export
#' @examples
#' # Example for sampler.spark function
#' # Define variable types for sampler
#' #var_types <- c(
#'  # age = "Continuous_int",
#'  # income = "Continuous_int",
#'  # education = "Nominal"
#' #)
#'
#' # Create initial imputation (simple mean/mode)
#' #imp_init <- sdf %>%
#'  # mutate(
#'  #   age = ifelse(is.na(age), 35, age),
#'  #   income = ifelse(is.na(income), 60000, income),
#'  #   education = ifelse(is.na(education), "Medium", education)
#'  # )
#'
#' # Run sampler
#' #sampled_data <- sampler.spark(
#'  # sc = sc,
#'  # data = sdf,
#'  # imp_init = imp_init,
#'  # fromto = c(1, 2),
#'  # var_types = var_types,
#'  # printFlag = TRUE,
#'  # checkpointing = FALSE
#' #)
#'
#' # View results
#' #sampled_data %>% collect()

sampler.spark <- function(sc,
                          data,
                          imp_init,
                          fromto,
                          var_types,
                          ud_methods = NULL,
                          predictorMatrix = NULL,
                          checkpointing,
                          checkpoint_frequency,
                          printFlag){


  #TODO; add support for functionalities present in the mice() function (where, ignore, blocks, predictorMatrix, formula, ...)

  # For iteration k in fromto
  from = fromto[1]
  to = fromto[2]

  var_names <- names(sparklyr::sdf_schema(data))

  # Method dictionary for imputation. Can change as desired
  # TODO: implement; keep this as default, or use user-provided dict ?

  if(is.null(ud_methods)){
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
  }
  else{
    print("Using user-defined imputation methods")
    imp_methods <- ud_methods
    names(imp_methods) <- var_names
  }

  num_vars <- length(var_names)
  # print("**DEBUG**: imp_methods:")
  # initialize the result with the initial imputation (mean or random)
  result <- imp_init

  for (k in from:to){
    cat("\n iteration: ", k,"\n")

    # For each variable j in the data
    j <- 0
    for (var_j in var_names){
      j <- j + 1
      cat("\n",j,"/",num_vars,"Imputing variable", var_j,"using method ")

      method <- imp_methods[[var_j]]
      cat(method)

      # Obtain the variables use to predict the missing values of variable j and create feature column
      label_col <- var_j # string object

      # DEFAULT: all other variables
      feature_cols <- setdiff(var_names, label_col)
      # remove the features with "none" imputation method
      feature_cols <- feature_cols[which(imp_methods[feature_cols] != "none")]

      # NON-DEFAULT: If predictorMatrix is provided, use it to select the features
      if(!is.null(predictorMatrix)){

        #Fetch the user-defined predictors for the label var_j
        UD_predictors <- colnames(predictorMatrix)[predictorMatrix[label_col, ]]
        #Check if the predictors are in the data
        if(length(UD_predictors) > 0){
          #If they are, use them as features
          feature_cols <- intersect(feature_cols, UD_predictors)
        }else{
          #If not, use stop
          cat(paste("The user-defined predictors for variable", label_col, "are not in the data or no predictors left after using user-defined predictors. Skipping Imputation for this variable.\n"))
          next
        }
      }else{
        #If not, use the default predictors
      }

      # Filter out Date data type (unsupported) (redundant?)
      feature_cols <- feature_cols[sapply(var_types[feature_cols],
                                          function(x) !(x %in% c("String", "smalldatetime")))]

      # Replace present values in label column with the original missing values
      # Is this done innefficiently (cbind)? Need to look into more optimized method maybe (spark native)
      j_df <- result %>%
        sparklyr::select(-label_col) %>%
        cbind(data %>% sparklyr::select(dplyr::all_of(label_col)))

      label_col_prev <- result %>% sparklyr::select(label_col)

      # Could this be avoided by passing in result to the impute function ? less select actions ?

      result <- switch(method,
         "Logistic" = impute_with_logistic_regression(sc, j_df, label_col, feature_cols),
         "Mult_Logistic" = impute_with_mult_logistic_regression(sc, j_df, label_col, feature_cols),
         "RandomForestClassifier" = impute_with_random_forest_classifier(sc, j_df, label_col, feature_cols),
         "Linear" = impute_with_linear_regression(sc=sc, sdf=j_df, target_col=label_col,
                                                  feature_cols=feature_cols, target_col_prev=label_col_prev),
         "RandomForestRegressor" = impute_with_random_forest_regressor(sc, sdf=j_df, target_col=label_col,
                                        feature_cols=feature_cols, target_col_prev=label_col_prev),
         "none" = j_df, # don't impute this variable
         "Invalid method"  # Default case, should never be reached
      ) # end of switch block

      # Add checkpointing here every 10 loop ? To avoid java.lang.StackOverflowError after 18ish variables
      if(j%%checkpoint_frequency == 0 & checkpointing){
        cat("\nMany variables, checkpointing to break the lineage\n")
        result <- sparklyr::sdf_checkpoint(result, eager=TRUE)
      }

    } # end of var_j loop (each variable) (1 iteration)

    # Checkpointing (Truncate lineage)
    if(checkpointing){
      #sparklyr::sdf_debug_string(result) # To print the lineage
      result <- sparklyr::sdf_checkpoint(result, eager=TRUE)
    }
  } # end of k loop (iterations)
  return(result)
} # end of sampler.spark function

#' MICE+ for Spark DataFrames using Sparklyr and Spark MLlib
#'
#' This function imputes missing values in a Spark DataFrame using MICE (Multiple Imputation by Chained Equations) algorithm. Additionally, it allows to look at the imputed values to see if they are reasonable and measure the uncertainty of the imputation.
#'
#' @importFrom dplyr %>%
#' @importFrom Matrix Matrix
#'
#' @param sc A Spark connection
#' @param data A Spark DataFrame, the original data with extra missing values
#' @param variable_types A named character vector, the variable types of the columns in the data.
#' @param analysis_formula A formula, the formula to use for the analysis
#' @param where_missing A logical vector, the locations of the missing values in the data
#' @param m The number of imputations to perform
#' @param method A character vector, the imputation method to use for each variable. If NULL, the function will infer the method based on the variable types.
#' @param predictorMatrix A matrix, the predictor matrix to use for the imputation. TBD
#' @param formulas A list, the formulas to use for the imputation. If NULL, the function will infer the formulas based on the other variables present in the data. TBD
#' @param modeltype A character vector, the model type to use for the imputation. If NULL, the function will infer the model type based on the variable types. The methods specified must match the order of the variables and must be one of "Logistic","Mult_Logistic","Linear","RandomForestClassifier","RandomForestRegressor" or "none".
#' @param maxit The maximum number of iterations to perform
#' @param printFlag A boolean, whether to print debug information
#' @param seed An integer, the seed to use for reproducibility
#' @param imp_init A Spark DataFrame, the original data with missing values, but with initial imputation (by random sampling or mean/median/mode imputation). Can be set to avoid re-running the initialisation step. Otherwise, the function will perform the initialisation step using the MeMoMe function.
#' @param checkpointing Default TRUE. Can be set to FALSE if you are running the package without access to a HDFS directory for checkpointing. It is strongly recommended to keep it to TRUE to avoid Stackoverflow errors.
#' @param checkpoint_frequency Advanced parameter, modify with care. If checkpointing = TRUE, how often to checkpoint , default = 10, so after processing every 10 variables, the lineage will be cut and the current state of computation will be save to disk. A low number might slow down computation but enable bigger computation. A number too high (or not checkpoiting) might cause JVM stackOverflowError as the lineage will have grown too big.
#' @param ... Additional arguments to be passed to the function. TBD
#' @return A list containing the Rubin's statistics for the model parameters, the per-imputation statistics, the imputation statistics, and the model parameters.
#' @export
#' @examples
#' # Example for mice.spark.plus function
#' # Create complete data (without extra missing values)
#' #complete_data <- data.frame(
#'  # outcome = c(1, 0, 1, 1, 0, 0),
#'  # age = c(25, 30, 35, 28, 45, 32),
#'  # income = c(50000, 60000, 70000, 55000, 80000, 52000),
#'  # education = c("High", "Medium", "High", "Low", "Low", "Medium")
#' #)
#'
#' # Copy complete data to Spark
#' #sdf_complete <- copy_to(sc, complete_data, "complete_data")
#'
#' # Create where_missing indicator (logical vector)
#' #where_missing <- c(FALSE, TRUE, TRUE, FALSE, TRUE, TRUE)  # Indicates artificially missing
#'
#' # Run MICE+
#' #mice_plus_result <- mice.spark.plus(
#'  # data = sdf,  # Data with missing values
#'  # data_true = sdf_complete,  # Complete data
#'  # sc = sc,
#'  # variable_types = variable_types,
#'  # analysis_formula = analysis_formula,
#'  # where_missing = where_missing,
#'  # m = 3,
#'  # maxit = 2,
#'  # printFlag = TRUE,
#'  # seed = 123,
#'  # checkpointing = FALSE
#' #)
#'
#' # View results including known missings
#' #mice_plus_result$rubin_stats
#' #mice_plus_result$known_missings[[1]] %>% collect()  # First imputation
#'
#' # Clean up
#' #spark_disconnect(sc)

mice.spark.plus <- function(data,
                       sc,
                       variable_types, # Used for initialization and method selection
                       analysis_formula,
                       where_missing,
                       m = 5,
                       method = NULL,
                       predictorMatrix = NULL,
                       formulas = NULL,
                       modeltype = NULL,
                       maxit = 5,
                       printFlag = TRUE,
                       seed = NA,
                       imp_init = NULL,
                       checkpointing = TRUE,
                       checkpoint_frequency = 10,
                       ...) {

  if (!is.na(seed)) set.seed(seed)

  from <- 1
  to <- from + maxit - 1

  # Get the formula for the model
  formula_obj <- analysis_formula
  param_names <- c("(Intercept)", all.vars(formula_obj)[-1])

  model_params <- vector("list", m)

  # To store per-imputation information
  imputation_stats <- vector("list", m)

  # To store the result imputations
  imputations <- list()

  # FOR EACH IMPUTATION SET i = 1, ..., m
  for (i in 1:m) {
    cat("\nStarting initialisation\n")

    init_start_time <- proc.time()

    imp_init <- init_with_random_samples(sc, data, column = NULL,
                                         checkpointing = checkpointing,
                                         checkpoint_frequency = checkpoint_frequency)

    init_end_time <- proc.time()
    init_elapsed <- (init_end_time - init_start_time)['elapsed']
    cat("Initalisation time:", init_elapsed)

    cat("\nImputation: ", i, "\n")
    # Run the imputation algorithm

    imp_start_time <- proc.time()
    imp <- sampler.spark(sc = sc,
                         data = data,
                         imp_init = imp_init,
                         fromto = c(from, to),
                         var_types = variable_types,
                         ud_methods = modeltype,
                         predictorMatrix = predictorMatrix,
                         printFlag = printFlag,
                         checkpointing = checkpointing,
                         checkpoint_frequency = checkpoint_frequency)

    imp_end_time <- proc.time()
    imp_elapsed <- (imp_end_time - imp_start_time)['elapsed']
    cat("\nImputation time:", imp_elapsed,".\n")

    pre_pred_cols <- c(colnames(data))
    post_pred_cols <- colnames(imp)
    extra_cols <- setdiff(post_pred_cols, pre_pred_cols)
    imp <- imp %>% dplyr::select(-dplyr::all_of(extra_cols))

    # Save imputed dataset
    imputations[[i]] <- imp

    #%%%% Analysis on the imputed data%%%%%
    # TODO: Add the functionality to choose what time of analysis, instead of logistic only
    model <- imp %>%
      sparklyr::ml_logistic_regression(formula = formula_obj)

    # Store model coefficients
    model_params[[i]] <- model$coefficients

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

      # Calculate Rubin's statistics. Source: TODO
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
    model_params = model_params,
    imputations = imputations
  ))
}

