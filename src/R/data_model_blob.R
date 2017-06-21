##########################################################
# Build multiple models                                  #
##########################################################

# Check the variable list
rxGetInfo(DF, getVarInfo = TRUE)

# Split the date into training/testing
splitFiles <- rxSplit(inData = DF, outFilesBase="DFsplit", splitByFactor="testSplitVar", 
                      transforms=list(testSplitVar = factor( 
                        sample(0:1, size=.rxNumRows, replace=TRUE, prob=c(.10, .9)), 
                        levels=0:1, labels = c("Test", "Train"))), overwrite = TRUE)
names(splitFiles)
rxSummary(~age, data = splitFiles[[1]], reportProgress = 0) #Test dataset
rxSummary(~age, data = splitFiles[[2]], reportProgress = 0) #Train dataset

# Create the training formula 
trainformula <- as.formula(paste("label~", paste(names(splitFiles[[2]])[c(2:19, 26:30)], collapse=' + ')))
trainformula

##########################################################
# Multi-class model evaluation metrics - manual          #
##########################################################

evaluate_model <- function(observed, predicted) {
  confusion <- table(observed, predicted)
  num_classes <- nlevels(observed)
  tp <- rep(0, num_classes)
  fn <- rep(0, num_classes)
  fp <- rep(0, num_classes)
  tn <- rep(0, num_classes)
  accuracy <- rep(0, num_classes)
  precision <- rep(0, num_classes)
  recall <- rep(0, num_classes)
  for(i in 1:num_classes) {
    tp[i] <- sum(confusion[i, i])
    fn[i] <- sum(confusion[-i, i])
    fp[i] <- sum(confusion[i, -i])
    tn[i] <- sum(confusion[-i, -i])
    accuracy[i] <- (tp[i] + tn[i]) / (tp[i] + fn[i] + fp[i] + tn[i])
    precision[i] <- tp[i] / (tp[i] + fp[i])
    recall[i] <- tp[i] / (tp[i] + fn[i])
  }
  overall_accuracy <- sum(tp) / sum(confusion)
  average_accuracy <- sum(accuracy) / num_classes
  micro_precision <- sum(tp) / (sum(tp) + sum(fp))
  macro_precision <- sum(precision) / num_classes
  micro_recall <- sum(tp) / (sum(tp) + sum(fn))
  macro_recall <- sum(recall) / num_classes
  metrics <- c("Overall accuracy" = overall_accuracy,
               "Average accuracy" = average_accuracy,
               "Micro-averaged Precision" = micro_precision,
               "Macro-averaged Precision" = macro_precision,
               "Micro-averaged Recall" = micro_recall,
               "Macro-averaged Recall" = macro_recall)
  return(metrics)
}

##########################################################
# Build 2 models                                         #
##########################################################

# Model-1: BTrees 
Tree1 <- rxBTrees(formula = trainformula, data = splitFiles[[2]], learningRate = 0.1, nTree=50, maxDepth = 5, seed = 1234, lossFunction = "multinomial")
Tree1

# Make predictions
prediction_df <- rxImport(inData = splitFiles[[1]])

Tree1_prediction <- rxPredict(Tree1, prediction_df, overwrite = TRUE)
rxGetInfo(prediction_df,  numRows = 5)

# Model evaluation
Tree1_metrics <- evaluate_model(observed = prediction_df$label, predicted = Tree1_prediction$label_Pred)
Tree1_metrics

# Model-2: Decision Forest
Forest1 <- rxDForest(formula = trainformula, data = splitFiles[[2]], nTree = 8, maxDepth = 32, mTry = 35, seed = 5)
Forest1

# Make predictions
Forest1_prediction <- rxPredict(Forest1, prediction_df, overwrite = TRUE)
rxGetInfo(prediction_df,  numRows = 5)

# Model evaluation
Forest1_metrics <- evaluate_model(observed = prediction_df$label, predicted = Forest1_prediction$label_Pred)
Forest1_metrics
