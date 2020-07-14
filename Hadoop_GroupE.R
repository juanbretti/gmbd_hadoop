## Libraries ----
library(odbc)
library(tidyverse)
library(skimr)
library(caret)
library(MASS)
library(mltest)
library(pROC)
library(gganimate)

## Connecting using ODBC ----

# https://db.rstudio.com/databases/hive
# https://db.rstudio.com/best-practices/drivers/
# Hive ODBC driver - https://www.progress.com/odbc/hortonworks-hive; # download the driver here
# https://www.progress.com/download/thank-you?interface=odbc&ds=hortonworks-hive&os=win-64

con <- DBI::dbConnect(odbc::odbc(),
                      Driver = "DataDirect 8.0 Apache Hive Wire Protocol",
                      Host   = "34.251.237.234",
                      Schema = "hadoop_2020_group_e",
                      UID    = 'juanbretti',
                      PWD    = '******',
                      Port   = 10000)

data_raw <- dbGetQuery(con, "SELECT TYPE, 
                       SUM(newbalanceOrig - oldbalanceOrg) AS Orig, 
                       SUM(newbalanceDest - oldbalanceDest) AS Dest, 
                       SUM((newbalanceDest - oldbalanceDest) + (newbalanceOrig - oldbalanceOrg)) as IndividualDifferenceBetweenDestOrig 
                       FROM `hadoop_2020_group_e`.`transactions` 
                       WHERE isfraud = TRUE 
                       GROUP BY TYPE 
                       ORDER BY IndividualDifferenceBetweenDestOrig DESC 
                       LIMIT 10;")

data_raw <- dbGetQuery(con, "SELECT * FROM `hadoop_2020_group_e`.`transactions`;")

dbDisconnect(con) # to disconnect from the DB

## Alternative, reading from the CSV ----
# data_raw <- read.csv(file.path('data','PS_20174392719_1491204439457_log.csv'))
# saveRDS(object = data_raw, file = file.path('storage', 'data.RData'))
# data_raw <- readRDS(file = file.path('storage', 'data.RData')) %>% 
#   mutate_at(vars(isFraud, isFlaggedFraud), as.logical)

## Pre processing ----

data_raw_pre <- data_raw %>% 
  mutate_at(vars(amount, oldbalanceOrg, newbalanceOrig, oldbalanceDest, newbalanceDest), function(x) log10(x+1)) %>% 
  mutate(hour = round(((step/24)%%1)*24),
         day = ceiling(step/24)) %>% 
  mutate(hour = factor(hour, labels = 0:24, levels = 0:24),
         day = factor(day, labels = 1:31, levels = 1:31)) %>% 
  mutate(incrementOrg = ifelse(oldbalanceOrg == 0, 1e4, newbalanceOrig / oldbalanceOrg),
         incrementDest = ifelse(oldbalanceDest == 0, 1e4, newbalanceDest / oldbalanceDest)) %>% 
  mutate_at(vars(isFraud, type), factor) %>% 
  dplyr::select(-nameOrig, -nameDest, -isFlaggedFraud, -step)

# Scale model
set.seed(42)
train_index <- createDataPartition(data_raw_pre$isFraud, p = 0.01, list = FALSE)
data_train <- data_raw_pre[ train_index,]
data_test  <- data_raw_pre[-train_index,]
# rm(list = c('data_raw'))

model_pre <- preProcess(data_train, method = c("center", "scale"))
data_train_pre <- predict(model_pre, data_train)
data_test_pre <- predict(model_pre, data_test)

# Exploratory data analysis

skim(data_train_pre)

data_raw_pre %>% 
  filter(isFraud == TRUE) %>% 
  group_by(hour) %>% 
  summarise(n=n()) %>% 
  ggplot() +
  geom_bar(aes(x = hour, y = n), stat="identity") +
  xlab('Hour of the day') +
  ylab('Number of transactions, log scale')

data_raw_pre %>% 
  filter(isFraud == TRUE) %>% 
  group_by(day) %>% 
  summarise(n=n()) %>% 
  ggplot() +
  geom_bar(aes(x = day, y = n), stat="identity") +
  xlab('Day of the month') +
  ylab('Number of transactions, log scale')

data_raw_pre %>% 
  group_by(hour, isFraud) %>% 
  summarise(n=n()) %>% 
  ggplot() +
  geom_bar(aes(x = hour, y = n, fill = isFraud), stat="identity") +
  scale_y_log10() +
  xlab('Hour of the day') +
  ylab('Number of transactions, log scale')

data_raw_pre %>% 
  group_by(day, isFraud) %>% 
  summarise(n=n()) %>% 
  ggplot() +
  geom_bar(aes(x = day, y = n, fill = isFraud), stat="identity") +
  scale_y_log10() +
  xlab('Day of the month') +
  ylab('Number of transactions, log scale')

# Animation
plot_ <- data_train_pre %>% 
  ggplot() +
  geom_point(aes(x = oldbalanceOrg, y = oldbalanceDest, size = incrementDest, colour = -isFraud)) +
  facet_wrap(~type) + 
  xlab('Origin new balance') +
  ylab('Destination new balance') +
  theme(legend.position = "none",
        axis.title.x=element_blank(),
        axis.text.x=element_blank(),
        axis.ticks.x=element_blank(),
        axis.title.y=element_blank(),
        axis.text.y=element_blank(),
        axis.ticks.y=element_blank()) +
  transition_time(as.numeric(day)) +
  enter_grow() + enter_fade() +
  exit_fade() +
  labs(title = "Day: {round(frame_time, 0)}")

animate(plot_, fps=5)
anim_save(file.path('storage', 'animation.gif'), animation = last_animation())

# Summary
# skim(data_train_pre)
# skim(data_test_pre)

# Weights to balance Fraud
temp <- data_train_pre$isFraud %>% 
  table(.) %>% 
  as.numeric(.)
data_weights <- ifelse(data_train_pre$isFraud == TRUE, sum(temp)/2/temp[2], sum(temp)/2/temp[1])

## Classification model ----

model_full <- glm(isFraud ~ ., data = data_train_pre, family = binomial(link = "logit"), weights = data_weights)
summary(model_full)

model_step <- model_full %>% 
  stepAIC(trace = TRUE)
summary(model_step)

## Table ----

logit_table <- function(x, level = 0.95) {
  table <- cbind(
    summary(x)$coefficients,
    exp(coefficients(x)),
    exp(confint.default(x, level = level)))
  # colnames(table)[1] <- "Variable"
  colnames(table)[5] <- "Exp(Beta)"
  return(table)
}

logit_table(model_step)

## Looking for the optimal value of threshold ----
# https://stats.stackexchange.com/questions/110969/using-the-caret-package-is-it-possible-to-obtain-confusion-matrices-for-specific
# https://community.rstudio.com/t/how-to-choose-best-threshold-value-automatically/12910
probsTrain <- predict(model_step, newdata = data_train_pre)
rocCurve <- roc(response = data_train_pre$isFraud, predictor = probsTrain, levels = levels(data_train_pre$isFraud))
plot(rocCurve, print.thres = "best", main = 'Fraud detection')
# max(sensitivities + specificities)

# Optimal from ROC curve
# pROC::coords(rocCurve, "best", input = "threshold", transpose = FALSE)
ROC_best <- coords(rocCurve, "best", ret = "all", transpose = FALSE)
print(ROC_best)
# All the points of the curve
coords(rocCurve, seq(0,1, by = 0.1), ret = 'all', transpose = FALSE)

## Imbalance ----
# F0.5 calculated as: 1.25*(recall*precision/(0.25*precision+recall))
# https://stats.stackexchange.com/questions/49226/how-to-interpret-f-measure-values
# https://machinelearningmastery.com/tour-of-evaluation-metrics-for-imbalanced-classification/
# https://stats.stackexchange.com/a/207371/80897

optimal_value <- function(x, measure = 'F0.5') {
  pdata <- predict(model_step, newdata = data_train_pre, type = "response")
  pdata <- as.numeric(pdata>=x)
  pdata <- factor(pdata, levels = c(0, 1), labels = c(FALSE, TRUE))
  out <- ml_test(pdata, data_train_pre$isFraud, output.as.table = TRUE)['TRUE', measure]
  return(out)
}

optimal_f1 <- optimize(optimal_value, interval=c(0, 1), maximum=TRUE, measure = 'F1')
optimal_f05 <- optimize(optimal_value, interval=c(0, 1), maximum=TRUE, measure = 'F0.5')
optimal_f2 <- optimize(optimal_value, interval=c(0, 1), maximum=TRUE, measure = 'F2')

## Optimal using weights

optimal_value <- function(x, m = c(1,1,5,10)) {
  # Using the threshold
  pdata <- predict(model_step, newdata = data_train_pre, type = "response")
  pdata <- as.numeric(pdata>=x)
  pdata <- factor(pdata, levels = c(0, 1), labels = c(FALSE, TRUE))
  # Economic table
  table_adjusted <- table(data = pdata, reference = data_train_pre$isFraud) * matrix(m, ncol = 2, nrow = 2)
  # Measurements
  TP <- diag(table_adjusted)
  FP <- rowSums(table_adjusted) - TP
  FN <- colSums(table_adjusted) - TP
  TN <- sapply(1:length(TP), function(y, TP) {
    sum(TP[-y], na.rm = TRUE)
  }, TP)
  accuracy <- sum(TP)/sum(table_adjusted, na.rm = TRUE)
  precision <- TP/(TP + FP)
  recall <- TP/(TP + FN)
  specificity <- TN/(TN + FP)
  F0.5 <- 1.25 * (recall * precision/(0.25 * precision + recall))
  F1 <- 2 * (precision * recall/(precision + recall))
  F2 <- 5 * (precision * recall/(4 * precision + recall))
  return(F0.5['TRUE'])
}

optimal_f05_w <- optimize(optimal_value, interval=c(0, 1), maximum=TRUE)

## Alternatives for optimal ----

tibble(
  ROC = ROC_best$threshold,
  F1 = optimal_f1$maximum,
  F0.5 = optimal_f05$maximum,
  F2 = optimal_f2$maximum,
  `F0.5 weighted` = optimal_f05_w$maximum
)

## Confusion matrix: Train ----
# Define threshold
pdata <- predict(model_step, newdata = data_train_pre, type = "response")
pdata <- as.numeric(pdata>=optimal_f05$maximum)
pdata <- factor(pdata, levels = c(0, 1), labels = c(FALSE, TRUE))

# Confusion matrix
(confusion_matrix <- caret::confusionMatrix(data = pdata, reference = data_train_pre$isFraud, positive = 'TRUE', mode = "everything"))

# Plot 1
# https://stackoverflow.com/questions/23891140/r-how-to-visualize-confusion-matrix-using-the-caret-package/42940553
fourfoldplot(confusion_matrix$table, color = c("#CC6666", "#99CC99"), margin = 2, main = "Confusion Matrix")

# Plot 2
# https://stackoverflow.com/questions/23891140/r-how-to-visualize-confusion-matrix-using-the-caret-package/42940553
draw_confusion_matrix <- function(cm) {
  
  total <- sum(cm$table)
  res <- as.numeric(cm$table)
  
  # Generate color gradients. Palettes come from RColorBrewer.
  greenPalette <- c("#F7FCF5","#E5F5E0","#C7E9C0","#A1D99B","#74C476","#41AB5D","#238B45","#006D2C","#00441B")
  redPalette <- c("#FFF5F0","#FEE0D2","#FCBBA1","#FC9272","#FB6A4A","#EF3B2C","#CB181D","#A50F15","#67000D")
  getColor <- function (greenOrRed = "green", amount = 0) {
    if (amount == 0)
      return("#FFFFFF")
    palette <- greenPalette
    if (greenOrRed == "red")
      palette <- redPalette
    colorRampPalette(palette)(100)[10 + ceiling(90 * amount / total)]
  }
  
  # set the basic layout
  layout(matrix(c(1,1,2)))
  par(mar=c(2,2,2,2))
  plot(c(100, 345), c(300, 450), type = "n", xlab="", ylab="", xaxt='n', yaxt='n')
  title('CONFUSION MATRIX', cex.main=2)
  
  # create the matrix 
  classes = colnames(cm$table)
  rect(150, 430, 240, 370, col=getColor("green", res[1]))
  text(195, 435, classes[1], cex=1.2)
  rect(250, 430, 340, 370, col=getColor("red", res[3]))
  text(295, 435, classes[2], cex=1.2)
  text(125, 370, 'Predicted', cex=1.3, srt=90, font=2)
  text(245, 450, 'Actual', cex=1.3, font=2)
  rect(150, 305, 240, 365, col=getColor("red", res[2]))
  rect(250, 305, 340, 365, col=getColor("green", res[4]))
  text(140, 400, classes[1], cex=1.2, srt=90)
  text(140, 335, classes[2], cex=1.2, srt=90)
  
  # add in the cm results
  text(195, 400, res[1], cex=1.6, font=2, col='white')
  text(195, 335, res[2], cex=1.6, font=2, col='black')
  text(295, 400, res[3], cex=1.6, font=2, col='black')
  text(295, 335, res[4], cex=1.6, font=2, col='black')
  
  # add in the specifics 
  plot(c(100, 0), c(100, 0), type = "n", xlab="", ylab="", main = "DETAILS", xaxt='n', yaxt='n')
  text(10, 85, names(cm$byClass[1]), cex=1.2, font=2)
  text(10, 70, round(as.numeric(cm$byClass[1]), 3), cex=1.2)
  text(30, 85, names(cm$byClass[2]), cex=1.2, font=2)
  text(30, 70, round(as.numeric(cm$byClass[2]), 3), cex=1.2)
  text(50, 85, names(cm$byClass[5]), cex=1.2, font=2)
  text(50, 70, round(as.numeric(cm$byClass[5]), 3), cex=1.2)
  text(70, 85, names(cm$byClass[6]), cex=1.2, font=2)
  text(70, 70, round(as.numeric(cm$byClass[6]), 3), cex=1.2)
  text(90, 85, names(cm$byClass[7]), cex=1.2, font=2)
  text(90, 70, round(as.numeric(cm$byClass[7]), 3), cex=1.2)
  
  # add in the accuracy information 
  text(30, 35, names(cm$overall[1]), cex=1.5, font=2)
  text(30, 20, round(as.numeric(cm$overall[1]), 3), cex=1.4)
  text(70, 35, names(cm$overall[2]), cex=1.5, font=2)
  text(70, 20, round(as.numeric(cm$overall[2]), 3), cex=1.4)
}

draw_confusion_matrix(confusion_matrix)

## Confusion matrix: Test ----
# Define threshold
pdata <- predict(model_step, newdata = data_test_pre, type = "response")
pdata <- as.numeric(pdata>=optimal_f05$maximum)
pdata <- factor(pdata, levels = c(0, 1), labels = c(FALSE, TRUE))

# Confusion matrix
(confusion_matrix <- caret::confusionMatrix(data = pdata, reference = data_test_pre$isFraud, positive = 'TRUE', mode = "everything"))

# Plot 1
fourfoldplot(confusion_matrix$table, color = c("#CC6666", "#99CC99"), margin = 2, main = "Confusion Matrix")

# Plot 2
draw_confusion_matrix(confusion_matrix)

## Plot per type ----

plot_confusion_matrix <- function(x) {
  # x <- 'PAYMENT'
  data_ <- filter(data_test_pre, type == x)
  pdata <- predict(model_step, newdata = data_, type = "response")
  pdata <- as.numeric(pdata>=optimal_f05$maximum)
  pdata <- factor(pdata, levels = c(0, 1), labels = c(FALSE, TRUE))
  # Confusion matrix
  confusion_matrix <- caret::confusionMatrix(data = pdata, reference = data_$isFraud, positive = 'TRUE', mode = "everything")
  # Plot 1
  plot1 <- fourfoldplot(confusion_matrix$table, color = c("#CC6666", "#99CC99"), margin = 2, main = "Confusion Matrix")
  # Plot 2
  plot2 <- draw_confusion_matrix(confusion_matrix)
  return(list(
    cm = confusion_matrix,
    plot1 = plot1,
    plot2 = plot2
  ))
}

data_test_pre %>% 
  filter(isFraud == TRUE) %>%
  group_by(type) %>% 
  summarise(n = n(), .groups = 'drop')

table(dplyr::select(data_test_pre, type, isFraud))

# Fraud
plot_confusion_matrix('TRANSFER')$cm
plot_confusion_matrix('CASH_OUT')$cm
