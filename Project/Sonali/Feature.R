library(readr)
d1 <- read.csv(file="/Users/sonalichaudhari/Desktop/ADS_Project/Data/Data2016_Cleaned.csv", header=TRUE, sep=",")
d2 <-read.csv(file="/Users/sonalichaudhari/Desktop/ADS_Project/Data/Data2017_Cleaned.csv", header=TRUE, sep=",")
data <- rbind(d1,d2)
library(ggplot2)
data$Month <- as.factor(data$Month)
g<-ggplot(data, aes(x=Month, fill=Month))+geom_bar()
ggplot(data, aes(x=DayofMonth, fill=DayofMonth))+geom_bar()
plot(data$Month)
g
p <- ggplot(data, aes(x=CRSElapsedTime, y=ActualElapsedTime))+geom_point(alpha=0.3, colour="blue")+geom_abline(colour="red", intercept=0, slope=1)
p

library(MASS)
library(ISLR)
fix(data)
names(data)
lm.fit = lm(Flight_Status~., data = data)
summary(lm.fit)
