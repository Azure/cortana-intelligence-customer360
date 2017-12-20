# Training the ML model
## Table of Contents
- [Pre-requisites](#pre-requisites)
- [Description of Scripts](#description-of-scripts)
- [Technique](#technique)
    * [1. Ingest Data and Create Data Summaries ](#1-ingest-data-and-create-data-summaries)
    * [2. Build and Evaluate Model](#2-build-and-evaluate-model)
    * [3. Deploy Model](#3-deploy-model)
- [ML Summary](#ml-summary)


## Pre-requisites
The following are required before continuing:
1. An Azure HDInsight **(Linux)** cluster running [Microsoft R Server][LINK_MRS].  
2. [Cleansed dataset][LINK_CLEANDATA] for modelling and validation.
3. Access to **R Studio** (or your favorite R environment) that connects to the MRS edge node and has permissions to run R scripts. The following are options:
	* [R Studio on Azure Portal](https://portal.azure.com)
	* [R Client](https://msdn.microsoft.com/en-us/microsoft-r/r-client-get-started)
	* R commandline on MRS edge node

Once development environment setup is complete, you will run three (3) R scripts in the specified order that: 
* Ingest the data into MRS environment. 
* Build a model.
* Deploy the trained model.  

## Sample Dataset
- [Customer_profile_label.csv][LINK_CLEANDATA]  

## Description of Scripts
1. [data_ingest_blob.R][SCRIPT_INGEST] – Ingests the dataset from an external blob to the MRS edge node and creates data summaries. 
	* **Summarization is a process of creating aggregates over various dimensions of the data**. 
	
1. [data_model_blob.R][SCRIPT_MODEL] – Splits the dataset into train/test datasets and then builds 2 classification models and evaluates these models. 

3. [data_deploy_blob.R][SCRIPT_DEPLOY] – Operationalizes and deploys model using mrsdeploy. 


## Technique
### 1. Ingest Data and Create Data Summaries
Data needs to be accessible, from underlying HDFS (wasb), to the edge node. Replace `myNameNode` and `bigDataDirRoot` to point to the **Azure Storage container** and **directory**, respectively, holding the sample data **(customer_profile_label.csv)** 

```R
# if the data is stored in a different Blob container than the default one, 
# the connection must be set up through the "wasb" string.
myNameNode <- "wasb://CONTAINER-NAME@STORAGE-ACCOUNT-NAME.blob.core.windows.net" 
myPort <- 0

# Define the path to the data.
bigDataDirRoot <- <path_to_dir>  
tempDir <- "mrs/temp"  # Store temporary XDF files.

```

Next make the directory on HDFS and define our filesystem path 
```R
# Create a new path in HDFS.
rxHadoopMakeDir(file.path(myNameNode, bigDataDirRoot, tempDir))

# Define HDFS file system.
hdfsFS <- RxHdfsFileSystem(hostName = myNameNode, port = myPort)
```

Define the input dataset schema

```R
# Define the columns of the input file
customer_profileColClasses <- list(customerID = list("character"), 
                                   age = list("numeric"),
                                   Gender = list("factor"), 
                                   Income = list("factor"),
                                   HeadofHousehold = list("factor"), 
                                   number_household = list("numeric"),
                                   months_residence = list("numeric"), 
                                   … 
                                   label = list("factor")
                                   )


# get all the column names
varNames <- names(customer_profileColClasses)

```
At this point we are ready to import the data for training. 

```R
# Define the text data source in hdfs
customerprofileData <- RxTextData(file = file.path("/data/customer_profile_label.csv"), delimiter = ",", fileSystem = hdfsFS)

rxGetInfo(customerprofileData, getVarInfo = TRUE)

XDF <- file.path(tempdir(), "customerprofile.xdf")
temp <- rxDataStep(customerprofileData, outFile = XDF, overwrite = TRUE, 
                   colInfo = list( Gender = list(type = "factor", levels = c("M", "F")), 
                                   Income = list(type = "factor", levels = c("Less_than_10K","10K_50K","50K_100K","Greater_than_100K")),
                                   label = list(type = "factor", levels = c("0","1","2","3"))))

rxGetInfo(temp, getVarInfo = TRUE)

#Import data to the MRS edge node
DF <- rxImport(temp)

#Check variable info
rxGetInfo(DF, getVarInfo = TRUE)
```
With data on the edge node, it is useful to explore the data distribution for important features, correlations`rxHistogram`, `rxSummary`, `rxCrossTabs`, `rxCor`.  The following are example explorations we have performed on the data. 

```R
#Preliminary tabulation/summary of the data
rxHistogram( ~r_60d, numBreaks = 25, data = DF) 
rxHistogram( ~f_60d, numBreaks = 25, data = DF) 

rxSummary(~ age + Gender + Income + T1count_30d + T2count_30d + T3count_30d + label, data = DF)

rxCrossTabs(T1spend_30d ~ Gender:Income, data = DF)
rxCrossTabs(T2spend_30d ~ Gender:Income, data = DF)
rxCrossTabs(T3spend_30d ~ Gender:Income, data = DF)

Cor <- rxCor(formula=~age + Gender + HeadofHousehold + number_household + months_residence + T1count_30d + T2count_30d + T3count_30d + 
               T1spend_30d + T2spend_30d + T3spend_30d + T1count_10d + T2count_10d + T3count_10d + T1spend_10d + T2spend_10d + T3spend_10d +
               r_60d + f_60d + T1_m_60d + T2_m_60d + T3_m_60d + T1count_3d + T2count_3d + T3count_3d + T1spend_3d + T2spend_3d + T3spend_3d + label, 
             data = DF, pweightsb= "perwt")
```
> **General guidance from a domain expert is ideal when deciding on  variables and possible interaction variables that are to be used while building a model.** 

> NOTE: For big datasets, you will need to point directly to the data on HDFS. The current data is small enough to be imported.  


### 2. Build and Evaluate Model
The data is now accessible from the edge node and visible to the MRS compute context. Split the data into 90% **Training** and 10% **Test**. 

> Depending on the business requirement, choose the optimal train/test split ratio. 

```R
#Check the variable list
rxGetInfo(DF, getVarInfo = TRUE)

#Split the data into training/testing
splitFiles <- rxSplit(inData = DF, outFilesBase="DFsplit", splitByFactor="testSplitVar", 
                      transforms=list(testSplitVar = factor( 
                        sample(0:1, size=.rxNumRows, replace=TRUE, prob=c(.10, .9)), 
                        levels=0:1, labels = c("Test", "Train"))), overwrite = TRUE)
names(splitFiles)
rxSummary(~age, data = splitFiles[[1]], reportProgress = 0) #Test dataset
rxSummary(~age, data = splitFiles[[2]], reportProgress = 0) #Train dataset

# create the training formula 
trainformula <- as.formula(paste("label~", paste(names(splitFiles[[2]])[c(2:19, 26:30)], collapse=' + ')))

```
`rxBTrees` algorithm is used to train the model. It scales the Gradient Boosting Machine (`gbm()`) that successfully handles classification and regression problems. Its implementation idea is built around decision trees (specifically `rxDTrees`) and stems from the belief that combining weak learners in an additive and iterative manner can produce accurate classifiers that are resistant to overfitting. Since customer profile enrichment requires integrating learnings about a customer from diverse data (whose individual meanings are not strong enough to make a glaring meaning), this model worked the best for our use case. 

```R
#BTrees 
trained_model <- rxBTrees(formula = label ~ age + Gender + Income + number_household + months_residence 
                  + T1count_30d + T2count_30d + T3count_30d
                  + T1spend_30d + T2spend_30d + T3spend_30d 
                  + T1count_10d + T2count_10d + T3count_10d 
                  + T1spend_10d + T2spend_10d + T3spend_10d 
                  + r_60d + f_60d + T1_m_60d + T2_m_60d + T3_m_60d
                  , data = splitFiles[[2]], learningRate = 0.1, nTree=50, maxDepth = 5, seed = 1234, lossFunction = "multinomial")

#Make predictions
prediction_df <- rxImport(inData = splitFiles[[1]])

model_prediction <- rxPredict(trained_model, prediction_df, outData = 'predout.xdf', overwrite = TRUE)
rxGetInfo(prediction_df,  numRows = 5)

#Model evaluation
model_metrics <- evaluate_model(observed = prediction_df$label, predicted = model_prediction$label_Pred)
```

### 3. Deploy Model
The trained model needs to be operationalized now. But before you proceed, your cluster needs to be configured; find instructions [here](https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-r-server-get-started#using-microsoft-r-server-operationalization)

Now that R Server is configured for operationalization, install `mrsdeploy` package to deploy our trained model. This allows the model to be consumed as a web service and batch scoring **(Revo64 CMD BATCH)**  

```R
install.packages("mrsdeploy")
library(mrsdeploy)
```

Use the trained B-Tree to create a prediction function that will be deployed. 
```R
#Produce a prediction function that can use the model
deployed_model <- function(age, Gender, Income ,number_household, months_residence, T1count_30d,  T2count_30d, T3count_30d, T1spend_30d, T2spend_30d, T3spend_30d, T1count_10d, T2count_10d, 
                        T3count_10d, T1spend_10d, T2spend_10d, T3spend_10d, r_60d, f_60d, T1_m_60d, T2_m_60d, T3_m_60d) {
  library(RevoScaleR)
  newdata <- data.frame(age= age, Gender=Gender, Income=Income, number_household =number_household, months_residence = months_residence, 
                        T1count_30d = T1count_30d, T2count_30d = T2count_30d, T3count_30d = T3count_30d,
                        T1spend_30d = T1spend_30d , T2spend_30d = T2spend_30d , T3spend_30d = T3spend_30d,
                        T1count_10d = T1count_10d, T2count_10d = T2count_10d, T3count_10d = T3count_10d,
                        T1spend_10d = T1spend_10d , T2spend_10d = T2spend_10d , T3spend_10d = T3spend_10d,
                        r_60d = r_60d , f_60d = f_60d , T1_m_60d = T1_m_60d , T2_m_60d = T2_m_60d , T3_m_60d = T3_m_60d)
  outputs = rxPredict(Tree1, newdata, type = "prob")
  X0_prob = outputs[1]
  X1_prob = outputs[2]
  X2_prob = outputs[3]
  X3_prob = outputs[4]
  label_Pred = outputs[5]
  answer = as.data.frame(cbind(X0_prob, X1_prob, X2_prob, X3_prob, label_Pred))
}
```

Evaluate/test prediction of model.
```R
# function locally by printing results
print(deployed_model(29,'M', 'Greater_than_100K', 1, 5, 6787, 178, 221, 0, 0, 0, 3234, 85, 88, 0, 0, 0, 1, 23, 0,0,0))
```

Next deploy the trained model as a service. 
```R
remoteLogin(deployr_endpoint = "http://localhost:12800",
            username = '****', 
            password = '****', 
            session = FALSE,
            commandline = TRUE)

api <- publishService(
  "mrsservice",
  code = deployed_model,
  model = trained_model,
  inputs = list(age= "numeric", Gender= "character", Income= "character", number_household = "numeric", months_residence = "numeric", 
                T1count_30d = "numeric", T2count_30d = "numeric", T3count_30d = "numeric",
                T1spend_30d = "numeric", T2spend_30d = "numeric", T3spend_30d = "numeric",
                T1count_10d = "numeric", T2count_10d = "numeric", T3count_10d = "numeric",
                T1spend_10d = "numeric", T2spend_10d =  "numeric", T3spend_10d = "numeric",
                r_60d = "numeric", f_60d =  "numeric", T1_m_60d =  "numeric", T2_m_60d =  "numeric", T3_m_60d = "numeric"),
  outputs = list(answer = 'data.frame'),
  v = "v1.0.0"
)

```

> ** NOTE** 
> Previously deployed service must be deleted i.e. `deleteService("mrsservice", "v1.0.0")`

Test the service
```R
result <- api$deployed_model(29,'M', 'Greater_than_100K', 1, 5, 6787, 178, 221, 0, 0, 0, 3234, 85, 88, 0, 0, 0, 1, 23, 0,0,0)

# Check the return code
result$success
```

## ML Summary
Once the initial training of the model is complete, only the model based web service needs to be invoked to make predictions for the new dataset. In the event, the model needs to be retrained with new data, all 3 scripts can be re-run, and a new web service deployed for future predictions. 

<!--  Links -->
[LINK_MRS]: https://azure.microsoft.com/en-us/services/hdinsight/r-server/
[LINK_CLEANDATA]: https://github.com/Azure/cortana-intelligence-customer360/blob/master/Technical%20Deployment%20Guide/data/customer_profile_label.csv
[SCRIPT_INGEST]: https://github.com/Azure/cortana-intelligence-customer360/blob/master/Technical%20Deployment%20Guide/src/R/data_ingest_blob.R
[SCRIPT_MODEL]: https://github.com/Azure/cortana-intelligence-customer360/blob/master/Technical%20Deployment%20Guide/src/R/data_model_blob.R
[SCRIPT_DEPLOY]: https://github.com/Azure/cortana-intelligence-customer360/blob/master/Technical%20Deployment%20Guide/src/R/data_deploy_blob.R
