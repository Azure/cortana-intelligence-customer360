# accept command line arguments
# 1. Model to be used for scoring
# 2. Data Partititon of timeslice to be considered. 
#       - This is based on ADF timeslices. 
#       - ETL'd file will be ingested from this directory and a copy of scored file written under "Scored_Profile" folder
# 3. Output directory that maps to folder for DW external tables. i.e. DW External tables reads a csv under this folder for
#    for updated customer profiles based on ETL and final scoring.
# 3. R script for Scoring.
# example:
# /Tree1 /YYYY/MM/DD/HH/mm /data/final_enriched_profiles enriched_customer_profiles.csv Scoring.R
# usage example:
# Revo64 CMD BATCH "--args /Tree1 /2017/05/25/21/15 /data/final_enriched_profiles enriched_customer_profiles" scripts/Scoring.R /dev/stdout

args = (commandArgs(TRUE)) # accept the arguments from command line

if (length(args) < 4) {
	stop ("At least 4 argument must be supplied. 
          - 1st is the location of the model file on hdfs. 
          - 2nd is directory on hdfs for the output model storage. 
          - 3rd is the filename to save the results
          - 4th is the time slice path", 
          call.=TRUE) 
}

# Location of previously trained model file
modelFile <- args[1]

# Output file path for scored results
outputFilePath <- args[2]

# Output file name
outputFileName <- args[3]

# Time slice directory path from ETL
timeSliceDirectory <- args[4]

# [DEBUGGING PURPOSES] - Check the location of the input blob file to be imported
rxHadoopListFiles("/data")

# if the data is stored in a different Blob container than the default one, 
# the connection can be set up thru the "wasb" string
myNameNode <- "wasb:///" 
myPort <- 0

# Define the path to the data
bigDataDirRoot <- "/data"  # this location can be adjusted to your defined directory accordingly.
tempDir <- "mrs/temp"  # this is a path to store temporary XDF files.

# Create a new path in HDFS
rxHadoopMakeDir(file.path(myNameNode, bigDataDirRoot, tempDir))

# Define HDFS file system
hdfsFS <- RxHdfsFileSystem(hostName=myNameNode, port=myPort)

if (rxHadoopFileExists(modelFile) == FALSE) { #check if the model file exists on hdfs 
  stop (paste("Model file not found at location: ", modelFile, sep = ' ') , call.=TRUE) 
}
filePrefix <- "/tmp/"
localModelFolder <-'testingModel'

strsplit(modelFile,'/')->modelPathSplit # extract the filename
modelFileName <-  modelPathSplit[[1]] [length(modelPathSplit[[1]])]
localModelPath =  paste(localModelFolder,modelFileName, sep = "/")

# Copy model file locally after removing the existing file
# i.e. clean up existing objects in workspace
if (file.exists(localModelPath)==TRUE) { 
  file.remove(localModelPath)
}
dir.create(localModelFolder, showWarnings = FALSE) # Create local folder (this gets ignored if folder exists)
rxHadoopCopyToLocal(modelFile, localModelPath) # copy the model to this location
load(localModelPath) # load the model into an object named - Tree1


# Define the text data source in hdfs
# Rename the etl_file. RxTexData doesn't expand wildcard and needs a filename that exists
rxHadoopMove(paste(timeSliceDirectory, 'part-*.csv', sep = '/'), paste(timeSliceDirectory, 'etlProfiles.csv', sep = '/'))
inputFile <- paste(timeSliceDirectory, 'etlProfiles.csv', sep = '/')

# Verify file exists
if (!rxHadoopFileExists(inputFile)) {
  stop("[ERROR] - Renaming of ETL file failed. ", call.=TRUE)
}
customerprofileData <- RxTextData(file=inputFile, delimiter=",", fileSystem=hdfsFS)
XDF <- file.path(tempdir(), "customerprofile.xdf")
temp <- rxDataStep(customerprofileData, outFile = XDF, overwrite = TRUE, 
                   colInfo = list( gender = list(type = "factor", levels = c("M", "F")), 
                                   income = list(type = "factor", levels = c("Less_than_10K","10K_50K","50K_100K","Greater_than_100K")),
                                   label = list(type = "factor", levels = c("0","1","2","3"))))

DF <- rxImport(temp) # Import the file to the head node
outputs = rxPredict(Tree1, DF, type = "prob")

## SAVE UPDATED CUSTOMER PROFILE AS CSV
## Using head and rxGetInfo for debugging
head(outputs)
head(DF)

rxGetInfo(outputs)
rxGetInfo(DF)

# Actual Scoring process begins
# Drop old labels from dataset
colsToDrop <- c("label")
updated_DF <- DF[ , !(names(DF) %in% colsToDrop)]

names(updated_DF)
rxGetInfo(updated_DF)

# Updating the DF with new labels
updated_DF <- transform(DF, 
                        label = outputs$label_Pred, 
                        prob_categoryT0 = outputs$X0_prob, 
                        prob_categoryT1 = outputs$X1_prob, 
                        prob_categoryT2 = outputs$X2_prob, 
                        prob_categoryT3 = outputs$X3_prob )


names(updated_DF)
rxGetInfo(updated_DF, getVarInfo = TRUE)

fileName = 'enriched_customer_profile.csv' # name of the final csv file
localUpdatedCustomerProfileFile <- paste(getwd(), '/', fileName, sep = '') 
 
# Save updated profiles as CSV, appending to the CSV each time we score and using that to overwrite the version on blob
# There is no need to save the column name i.e. this is output to a polybase external table
write.table(updated_DF, file = localUpdatedCustomerProfileFile, row.names = FALSE, append = TRUE, col.names = FALSE, sep = ",")  

# Clean up final and backup files if they exist. One file should only exist per time slice
backUpFolder = paste(timeSliceDirectory, 'Scored_Result', sep = '/')

# Remove Final Scored file if already exists.
finalOutputFile = paste(outputFilePath, outputFileName, sep = '/')
isFileExist = rxHadoopFileExists(finalOutputFile)
if (isFileExist) {
  rxHadoopRemove(finalOutputFile)
}

#  One backup scored file should exist for one time slice
backupOutputFile = paste(backUpFolder, outputFileName, sep = '/')
isBackUpFileExists = rxHadoopFileExists(backupOutputFile)
if (isBackUpFileExists) {
  rxHadoopRemove(backupOutputFile)
}

# Ensure the output/backup directories if they don't already exist. 
# Fails quietly even if the directory exist
rxHadoopMakeDir(outputFilePath)
rxHadoopMakeDir(backUpFolder)

rxHadoopCopyFromLocal(localUpdatedCustomerProfileFile, finalOutputFile) 
rxHadoopCopyFromLocal(localUpdatedCustomerProfileFile, backupOutputFile)
