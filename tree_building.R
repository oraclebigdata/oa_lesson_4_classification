install.packages("rpart")
install.packages("randomForest")
install.packages("pmml")
library(rpart)
library(randomForest)
library(pmml)
library(parallel)
library(foreach)
library(doParallel)

weather_sample <- read.delim("/mnt/shared/decision_tree_bootcamp/weather_sample.tsv")
dec_tree <- rpart(weather ~ stn+wban+weather_month+weather_day+temp+dewp, data=weather_sample, method="class", cp=0.0001)
prune(dec_tree, cp=0.006)
plotcp(dec_tree)
plot(dec_tree, main="It's Always Sunny")
text(dec_tree, use.n=TRUE, cex=0.5)

cl <- makeCluster(detectCores())
registerDoParallel(cl)
rf <- foreach(ntree=rep(5,detectCores()), .combine=combine, .packages='randomForest') %dopar%
randomForest(weather ~ stn+wban+weather_month+weather_day+temp+dewp, data=weather_sample, importance=TRUE,
proximity=TRUE, ntree=ntree)
stopCluster(cl)
saveXML(pmml(rf), file="/home/oracle/weather_forest.rf.xml")
