# bigMICE <img src="man/figures/logo.png" align="right" height="200" alt="" />


<!-- badges: start -->
[![R-CMD-check](https://github.com/bigcausallab/bigMICE/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/bigcausallab/bigMICE/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->

bigMICE is an R package based on the `sparklyr` library, designed for handling large datasets with multiple imputation using an efficient and scalable approach.

## Setup and recommendations

### Spark and sparklyr

(Optional) Setting up an environment (run once in a new R project) 
```r
install.packages("renv")
library(renv)
renv::init()
```

Installing sparklyr and spark (run once). If not using the latest version of `sparklyr`, make sure to install a compatible Spark version, and vice versa. For the latest sparklyr release (1.9.1), the compatible Spark version is 4.0.0. For sparklyr versions < 1.9.0, you will need a spark version < 4.0.0.
```r
install.packages("sparklyr") # version 1.9.1
options(timeout = 6000)
library(sparklyr)
spark_install(version="4.0.0")
```

To check that the correct combination of Spark and sparklyr have been installed, use the following two commands:
```r
sparklyr::spark_installed_versions()
utils::packageVersion("sparklyr")
```


### Hadoop
For robust execution of Spark on big data sets, checkpointing can be needed. To make it possible to enable checkpointing, Hadoop needs to be installed. For smaller datasets or for running toy examples Hadoop installation can be skipped.

**On Linux**
[https://hadoop.apache.org/docs/r1.2.1/single_node_setup.html](https://hadoop.apache.org/docs/r1.2.1/single_node_setup.html)

**On Windows**
[https://gist.github.com/vorpal56/5e2b67b6be3a827b85ac82a63a5b3b2e](https://gist.github.com/vorpal56/5e2b67b6be3a827b85ac82a63a5b3b2e)

**Note** that specific Java versions are needed to run Spark: [https://spark.apache.org/docs/latest/](https://spark.apache.org/docs/latest/) (JDK 17/21 at the moment of writing)

<!--It is **strongly recommended** to also install Hadoop to have access to a HDFS (Hadoop Distributed File System) directory when using the package. This allows for checkpointing which extends the capabilities of the package. Here is an article on [how to install Hadoop on Windows](https://medium.com/analytics-vidhya/hadoop-on-windows-eb322f520168). [needs more resources].

If you do not have access to a HDFS directory, make sure to include the parameter checkpointing = FALSE when calling functions on the package.
-->

## Installation

To install bigMICE from GitHub, use the following command in R:

```r
# Install devtools if not already installed
install.packages("devtools")

# Install bigMICE from GitHub
devtools::install_github("bigcausallab/bigMICE")
```

Once installed, load the package:

```r
library(bigMICE)
```

## Example Usage

Loading necessary libraries:
```r
library(bigMICE)
library(dplyr)
library(sparklyr)
```
Creating a local Spark session.
```r
conf <- spark_config()
conf$`sparklyr.shell.driver-memory`<- "10G"
conf$spark.memory.fraction <- 0.8
conf$`sparklyr.cores.local` <- 4
#conf$`spark.local.dir` <- "/local/data/spark_tmp/" # needed for checkpointing.
# If not possible, add the parameter checkpointing = FALSE to the mice.spark call

sc = spark_connect(master = "local", config = conf)
```

Loading the dataset (specific dataset used can be found in the `mice` R package [here](https://github.com/amices/mice/tree/master/data))
```r
# Loading the data
#data <- load("boys.rda")
#write.csv(boys, "data.csv", row.names = FALSE)
sdf <- spark_read_csv(sc, "data", "data.csv", header = TRUE, infer_schema = TRUE, null_value = "NA") %>%
  select(-all_of(c("hgt","wgt","bmi","hc")))
# preparing the elements before running bigMICE
variable_types <- c(age = "Continuous_float", 
                   gen = "Nominal", 
                   phb = "Nominal",
                   tv = "Continuous_int",
                   reg = "Nominal")

analysis_formula <- as.formula("phb ~ age + gen + tv + reg")

```
Calling the mice.spark function to obtain m=1 imputed dataset:
```r
imputation_results <- bigMICE::mice.spark(data = sdf,
                                            sc = sc,
                                variable_types = variable_types,
                              analysis_formula = analysis_formula,
                               predictorMatrix = NULL,
                                             m = 1,
                                         maxit = 2,
                                 checkpointing = FALSE)

imputation_results$rubin_stats

imputation_results$model_params

imputation_results$imputation_stats
```

