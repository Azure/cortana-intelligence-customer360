##########################################################
# Data ingestion                                         #
##########################################################

# check the location of the input blob file to be imported
rxHadoopListFiles("/data")

# if the data is stored in a different Blob container than the default one, 
# the connection can be set up thru the "wasb" string
myNameNode <- "wasb://rba360-hdi2-2017-04-03t12-47-37-307z@rba360sa.blob.core.windows.net" 
myPort <- 0

# Define the path to the data
bigDataDirRoot <- "/data"  # this location can be adjusted to your defined directory accordingly.
tempDir <- "mrs/temp"  # this is a path to store temporary XDF files.

# Create a new path in HDFS
rxHadoopMakeDir(file.path(myNameNode, bigDataDirRoot, tempDir))

# Define HDFS file system
hdfsFS <- RxHdfsFileSystem(hostName = myNameNode, port = myPort)

# Define the column names and variable type of the input file
customer_profileColClasses <- list(customerID = list("character"), 
                                   age = list("numeric"),
                                   Gender = list("factor"), 
                                   Income = list("factor"),
                                   HeadofHousehold = list("factor"), 
                                   number_household = list("numeric"),
                                   months_residence = list("numeric"), 
                                   T1count_30d = list("numeric"), 
                                   T2count_30d = list("numeric"), 
                                   T3count_30d = list("numeric"), 
                                   T1spend_30d = list("numeric"), 
                                   T2spend_30d = list("numeric"), 
                                   T3spend_30d = list("numeric"), 
                                   T1count_10d = list("numeric"), 
                                   T2count_10d = list("numeric"), 
                                   T3count_10d = list("numeric"), 
                                   T1spend_10d = list("numeric"), 
                                   T2spend_10d = list("numeric"), 
                                   T3spend_10d = list("numeric"), 
                                   T1count_3d = list("numeric"), 
                                   T2count_3d = list("numeric"), 
                                   T3count_3d = list("numeric"), 
                                   T1spend_3d = list("numeric"), 
                                   T2spend_3d = list("numeric"), 
                                   T3spend_3d = list("numeric"), 
                                   r_60d = list("numeric"), 
                                   f_60d = list("numeric"), 
                                   T1_m_60d = list("numeric"),
                                   T2_m_60d = list("numeric"), 
                                   T3_m_60d = list("numeric"), 
                                   label = list("factor")
)


# List all the column names
varNames <- names(customer_profileColClasses)

# Define the text data source in hdfs
customerprofileData <- RxTextData(file = file.path("/data/customer_profile_label.csv"), delimiter = ",", fileSystem = hdfsFS)

rxGetInfo(customerprofileData, getVarInfo = TRUE)

# Convert some variables to factors
XDF <- file.path(tempdir(), "customerprofile.xdf")
temp <- rxDataStep(customerprofileData, outFile = XDF, overwrite = TRUE, 
                   colInfo = list( Gender = list(type = "factor", levels = c("M", "F")), 
                                   Income = list(type = "factor", levels = c("Less_than_10K","10K_50K","50K_100K","Greater_than_100K")),
                                   label = list(type = "factor", levels = c("0","1","2","3"))))

rxGetInfo(temp, getVarInfo = TRUE)

# Import the file to the edge node
DF <- rxImport(temp)

head(DF)
names(DF)

#Check variable info
rxGetInfo(DF, getVarInfo = TRUE)

##########################################################
# Data summaries                                         #
##########################################################

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

Cor

