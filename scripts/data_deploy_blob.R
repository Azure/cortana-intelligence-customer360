##########################################################
# Deploy model                                           #
##########################################################

# Install the mrsdeploy package if it is not already installed
#install.packages("mrsdeploy")
library(mrsdeploy)

# Model deployment with actual model
Tree1 <- rxBTrees(formula = label ~ age + Gender + Income + number_household + months_residence 
                  + T1count_30d + T2count_30d + T3count_30d
                  + T1spend_30d + T2spend_30d + T3spend_30d 
                  + T1count_10d + T2count_10d + T3count_10d 
                  + T1spend_10d + T2spend_10d + T3spend_10d 
                  + r_60d + f_60d + T1_m_60d + T2_m_60d + T3_m_60d
                  , data = splitFiles[[2]], learningRate = 0.1, nTree=50, maxDepth = 5, seed = 1234, lossFunction = "multinomial")
summary(Tree1)

# Make predictions locally
rxPredict(Tree1, data = splitFiles[[1]], outData = 'predout.xdf', overwrite = TRUE)
rxGetInfo('predout.xdf',  numRows = 5)

# Produce a prediction function that can use the above model
actualmodel <- function(age, Gender, Income ,number_household, months_residence, T1count_30d,  T2count_30d, T3count_30d, T1spend_30d, T2spend_30d, T3spend_30d, T1count_10d, T2count_10d, 
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

# Test function locally before deploying as a web service
print(actualmodel(29,	'M', 'Greater_than_100K', 1, 5, 6787, 178, 221, 0, 0, 0, 3234, 85, 88, 0, 0, 0, 1, 23, 0,0,0))

##########################################################
#            Log into Microsoft R Server                 #
##########################################################
# Use `remoteLogin` to authenticate with R Server using 
# the local admin account. Use session = false so no 
# remote R session started

remoteLogin(deployr_endpoint = "http://localhost:12800",
            username = 'admin', 
            password = 'HolaSenor123!', 
            session = FALSE,
            commandline = TRUE)

##########################################################
#             Publish Model as a Service                 #
##########################################################
# Publish as service using `publishService()` function from 
# `mrsdeploy` package. Name service "mtService" and provide
# unique version number. Assign service to the variable `api`

# Switch between local and remote sessions
#pause()
#resume()

# If the web service has already been deployed, delete it before re-deploying
#deleteService("mrsservice", "v1.0.0")

api <- publishService(
  "mrsservice",
  code = actualmodel,
  model = Tree1,
  inputs = list(age= "numeric", Gender= "character", Income= "character", number_household = "numeric", months_residence = "numeric", 
                T1count_30d = "numeric", T2count_30d = "numeric", T3count_30d = "numeric",
                T1spend_30d = "numeric", T2spend_30d = "numeric", T3spend_30d = "numeric",
                T1count_10d = "numeric", T2count_10d = "numeric", T3count_10d = "numeric",
                T1spend_10d = "numeric", T2spend_10d =  "numeric", T3spend_10d = "numeric",
                r_60d = "numeric", f_60d =  "numeric", T1_m_60d =  "numeric", T2_m_60d =  "numeric", T3_m_60d = "numeric"),
  outputs = list(answer = 'data.frame'),
  v = "v1.0.0"
)

##########################################################
#                 Consume Service in R                   #
##########################################################

# Print capabilities that define the service holdings: service 
# name, version, descriptions, inputs, outputs, and the 
# name of the function to be consumed
print(api$capabilities())
api

# Consume service by calling function
# contained in this service
result <- api$actualmodel(29,	'M', 'Greater_than_100K', 1, 5, 6787, 178, 221, 0, 0, 0, 3234, 85, 88, 0, 0, 0, 1, 23, 0,0,0)
result$success
result$outputParameters

# Print response output variables
print(result)

##########################################################
#       Get Swagger File for this Service in R Now       #
##########################################################

# During this authenticated session, download the  
# Swagger-based JSON file that defines this service
swagger <- api$swagger()
cat(swagger, file = "swagger.json", append = FALSE)

# Now you can share Swagger-based JSON so others can consume it

##########################################################
#          Delete service version when finished          #
##########################################################

# User who published service or user with owner role can
# remove the service when it is no longer needed
#status <- deleteService("mrsservice", "v1.0.0")
#status

##########################################################
#                   Log off of R Server                  #
##########################################################

# Log off of R Server
remoteLogout()

