rm(list=ls())
library(tidyverse)
library(data.table)
library(sparklyr)
library(rsparkling)
library(h2o)
library(lubridate)
library(zoo)
library(caret)




#Spark script
config <- spark_config()
config$'sparklyr.shell.executor-memory' <- "30g"
config$'sparklyr.shell.driver-memory' <- "10g"
# config$spark.yarn.am.memory <- "15g"
# 
# # Full list of config options: https://spark.apache.org/docs/2.0.1/running-on-yarn.html
# # 
# start<-Sys.time()
# 
sc <- spark_connect(master = "yarn-client", spark_home = "/usr/hdp/current/spark2-client/", config=config)
# 
in_path_spark = 'hdfs:///user/hpc3552/scene-csv/sample03/clean/'
# 
scene_mbr_dim <- spark_read_csv(sc, name='scene_mbr_dim', path=paste(in_path_spark, 'scene_mbr_dim.csv', sep=""), header = TRUE, delimiter = ",")
# 
scene_mbr_acct_dim <- spark_read_csv(sc, name='scene_mbr_acct_dim', path=paste(in_path_spark, 'scene_mbr_acct_dim.csv', sep=""), header = TRUE, delimiter = ",")

scene_pt_fact <- spark_read_csv(sc, name='scene_pt_fact', path=paste(in_path_spark, 'scene_pt_fact.csv', sep=""), header = TRUE, delimiter = ",")


#R script
in_path_r = '/global/project/queens-mma/scene-csv/sample0003/clean/'

#Read the regular R files for now
scene_mbr_dim <-fread(paste(in_path_r, 'scene_mbr_dim.csv', sep=""), sep=",")
scene_mbr_acct_dim <-fread(paste(in_path_r, 'scene_mbr_acct_dim.csv', sep=""), sep=",")
scene_pt_fact <-fread(paste(in_path_r, 'scene_pt_fact.csv', sep=""), sep=",")

# Get rid of high rollers right off the bat
#NOTE CHANGE THIS TO BE BASED ON STANDARD DEVIATION
scene_pt_fact_filter <- scene_pt_fact %>%
  filter(txn_amt <= 10000)

#Change dates
scene_pt_fact_filter$time_lvl_st_dt <- as.Date(scene_pt_fact_filter$time_lvl_st_dt)
scene_pt_fact_filter$mo_clndr_code <- as.factor(scene_pt_fact_filter$mo_clndr_code)

#Crate a df for data exploration
plot_data <- scene_pt_fact_filter %>% 
  filter(anul_fncl_code>=2013 & time_lvl_st_dt != "2016-09-01") %>% 
  group_by(time_lvl_st_dt) %>% 
  summarise(total = sum(txn_amt), 
            mean = mean(txn_amt), 
            n = n()) %>% 
  mutate(month = as.factor(format(time_lvl_st_dt, "%m")), 
         year = as.factor(format(time_lvl_st_dt, "%Y")))

ggplot(plot_data, aes(x = time_lvl_st_dt, y = total))+
  geom_point()+
  geom_line()+
  labs(x = "Date",
       y = "Total Monthly Spend ($)",
       subtitle = "Total monthly spending has consistently been on the rise since 2013; Substantial seasonality present")

ggplot(plot_data, aes(x = time_lvl_st_dt, y = mean))+
  geom_point()+
  geom_line()+
  labs(x = "Date",
       y = "Average Monthly Spend ($)",
       subtitle = "However, the average monthly spending has been trending downwards; Seasonality still present.")

ggplot(plot_data, aes(x = time_lvl_st_dt, y = n))+
  geom_point()+
  geom_line()+
  labs(x = "Date",
       y = "Total number of transactions",
       subtitle = "The descrepancy can be explained given the increasing number of transactions.")

ggplot(plot_data, aes(x = month, y = total, col = year))+geom_point()
ggplot(plot_data, aes(x = month, y = mean, col = year))+geom_point()

#Get most recent sequence
mbr_max_sequence <- scene_mbr_dim %>%
  group_by(scene_mbr_key)%>%
  arrange(desc(scene_mbr_key))%>%
  summarise(max_sequence = max(scene_mbr_seq_num))

scene_dim_test <- left_join(scene_mbr_dim, mbr_max_sequence, by = "scene_mbr_key") %>%
  filter(scene_mbr_seq_num == max_sequence)%>%
  select(-max_sequence)

#Test to make sure recent sequence code works
test <- scene_dim_test %>%
  group_by(scene_mbr_key) %>%
  summarise(n = n())

#Should print 1
max(test$n)

#Create first data frame with age of account

scene_mbr_acct_dim_test <- scene_mbr_acct_dim %>%
  mutate(date = as.Date(acct_eff_from_tmstamp),
         scene_mbr_key = prim_scene_mbr_key) %>%
  group_by(prim_scene_mbr_key) %>%
  select(scene_mbr_key, date)

# Would need to fix this
max_date <- max(scene_mbr_acct_dim_test$date)

scene_mbr_acct_dim_test <- scene_mbr_acct_dim_test %>% 
  mutate(account_age = as.integer(max_date - date)) %>%
  ungroup()%>%
  select(scene_mbr_key, account_age)

#Create second data frame from demographic information
scene_mbr_dim_test <- scene_mbr_dim %>% 
  mutate_if(is.character, as.factor) %>%
  mutate(age = 2016 - brth_dt)%>%
  select(scene_mbr_key,
         age,
         gndr_desc,
         ed_lvl_desc,
         mrtl_stat_desc,
         lang_desc,
         psnl_city)
         
#Create third data frame to profile the accounts
scene_pt_fact_filter_test <- scene_pt_fact_filter %>%
  group_by(scene_mbr_key) %>%
  summarise(mean = mean(txn_amt),
            sd = sd(txn_amt),
            n = n())

scene_pt_fact_filter_test

profile_test <- left_join(scene_pt_fact_filter_test, scene_mbr_dim_test, by = "scene_mbr_key") %>%
  left_join(scene_mbr_acct_dim_test, by = "scene_mbr_key")

transaction_df <- left_join(scene_pt_fact_filter, profile_test, by = "scene_mbr_key") %>%
  select(txn_amt,
         anul_clndr_code,
         mo_clndr_code,
         mean,
         sd,
         n,
         age,
         gndr_desc,
         ed_lvl_desc,
         mrtl_stat_desc,
         lang_desc,
         account_age,
         psnl_city
         )

transaction_df$anul_clndr_code <- as.factor(transaction_df$anul_clndr_code)

str(transaction_df)

ip <- "192.168.30.21"
port <- 60035

transaction_df <- transaction_df[!is.na(transaction_df$sd),]
transaction_df <- transaction_df[!is.na(transaction_df$age),]
transaction_df <- transaction_df[!is.na(transaction_df$account_age),]

sum(is.na(transaction_df))

fitControl <- trainControl(## 10-fold CV
  method = "repeatedcv",
  number = 10)

n = nrow(transaction_df)
trainIndex = sample(1:n, size = round(0.7*n), replace=FALSE)
train = transaction_df[trainIndex ,]
test = transaction_df[-trainIndex ,]



gbmFit1 <- train(txn_amt ~ ., data = train, 
                 method = "gbm", 
                 trControl = fitControl,
                 ## This last option is actually one
                 ## for gbm() that passes through
                 verbose = TRUE
                 )

pred <- predict(gbmFit1, test)






































# This table has the most recent month that a customer spent something
most_recent_month <- scene_pt_fact_filter %>%
  mutate(yr_month = as.Date(time_lvl_st_dt)) %>%
  group_by(scene_mbr_key) %>%
  summarise(most_recent_month = max(yr_month))

check <- most_recent_month %>%
  group_by(most_recent_month) %>%
  dplyr::summarise(n = n()) %>%
  dplyr::arrange(desc(n))


# This table has the monthly spending by customer
monthly_spend_by_mbr <- scene_pt_fact_filter %>%
  mutate(yr_month = as.Date(time_lvl_st_dt)) %>%
  group_by(scene_mbr_key, yr_month) %>%
  arrange(yr_month, desc) %>%
  summarise(most_recent_spending = sum(txn_amt))

# This table has the most recent monthly spend
most_recent_spend <- left_join(monthly_spend_by_mbr, 
                                         most_recent_month, 
                                         by = "scene_mbr_key") %>%
                     filter(yr_month == most_recent_month) %>%
                     select(-most_recent_month)

master_table <- most_recent_spend

# This table contains the one lagged month indicator
most_recent_month_minus_1 <- most_recent_month %>%
  mutate(lag1 = most_recent_month - months(1)) %>%
  select(-most_recent_month)


most_recent_spend_minus_1 <- left_join(monthly_spend_by_mbr, 
                               most_recent_month_minus_1, 
                               by = "scene_mbr_key") %>%
  filter(yr_month == lag1) %>%
  mutate(lag_1_spending = most_recent_spending)%>%
  select(-yr_month, -most_recent_spending, -lag1)

master_table <- left_join(master_table, 
                          most_recent_spend_minus_1,
                          by = "scene_mbr_key")

# This table contains the two lagged month indicator
most_recent_month_minus_2 <- most_recent_month %>%
  mutate(lag2 = most_recent_month - months(2)) %>%
  select(-most_recent_month)


most_recent_spend_minus_2 <- left_join(monthly_spend_by_mbr, 
                                       most_recent_month_minus_2, 
                                       by = "scene_mbr_key") %>%
  filter(yr_month == lag2) %>%
  mutate(lag_2_spending = most_recent_spending)%>%
  select(-yr_month, -most_recent_spending, -lag2)

master_table <- left_join(master_table, 
                          most_recent_spend_minus_2,
                          by = "scene_mbr_key")

# This table contains the three lagged month indicator
most_recent_month_minus_3 <- most_recent_month %>%
  mutate(lag3 = most_recent_month - months(3)) %>%
  select(-most_recent_month)


most_recent_spend_minus_3 <- left_join(monthly_spend_by_mbr, 
                                       most_recent_month_minus_3, 
                                       by = "scene_mbr_key") %>%
  filter(yr_month == lag3) %>%
  mutate(lag_3_spending = most_recent_spending)%>%
  select(-yr_month, -most_recent_spending, -lag3)

master_table <- left_join(master_table, 
                          most_recent_spend_minus_3,
                          by = "scene_mbr_key")

# This table contains the four lagged month indicator
most_recent_month_minus_4 <- most_recent_month %>%
  mutate(lag4 = most_recent_month - months(4)) %>%
  select(-most_recent_month)


most_recent_spend_minus_4 <- left_join(monthly_spend_by_mbr, 
                                       most_recent_month_minus_4, 
                                       by = "scene_mbr_key") %>%
  filter(yr_month == lag4) %>%
  mutate(lag_4_spending = most_recent_spending)%>%
  select(-yr_month, -most_recent_spending, -lag4)

master_table <- left_join(master_table, 
                          most_recent_spend_minus_4,
                          by = "scene_mbr_key")

# This table contains the five lagged month indicator
most_recent_month_minus_5 <- most_recent_month %>%
  mutate(lag5 = most_recent_month - months(5)) %>%
  select(-most_recent_month)


most_recent_spend_minus_5 <- left_join(monthly_spend_by_mbr, 
                                       most_recent_month_minus_5, 
                                       by = "scene_mbr_key") %>%
  filter(yr_month == lag5) %>%
  mutate(lag_5_spending = most_recent_spending)%>%
  select(-yr_month, -most_recent_spending, -lag5)

master_table <- left_join(master_table, 
                          most_recent_spend_minus_5,
                          by = "scene_mbr_key")

# This table contains the six lagged month indicator
most_recent_month_minus_6 <- most_recent_month %>%
  mutate(lag6 = most_recent_month - months(6)) %>%
  select(-most_recent_month)


most_recent_spend_minus_6 <- left_join(monthly_spend_by_mbr, 
                                       most_recent_month_minus_6, 
                                       by = "scene_mbr_key") %>%
  filter(yr_month == lag6) %>%
  mutate(lag_6_spending = most_recent_spending)%>%
  select(-yr_month, -most_recent_spending, -lag6)

master_table <- left_join(master_table, 
                          most_recent_spend_minus_6,
                          by = "scene_mbr_key")


# This table contains the seven lagged month indicator
most_recent_month_minus_7 <- most_recent_month %>%
  mutate(lag7 = most_recent_month - months(7)) %>%
  select(-most_recent_month)


most_recent_spend_minus_7 <- left_join(monthly_spend_by_mbr, 
                                       most_recent_month_minus_7, 
                                       by = "scene_mbr_key") %>%
  filter(yr_month == lag7) %>%
  mutate(lag_7_spending = most_recent_spending)%>%
  select(-yr_month, -most_recent_spending, -lag7)

master_table <- left_join(master_table, 
                          most_recent_spend_minus_7,
                          by = "scene_mbr_key")

# This table contains the eight lagged month indicator
most_recent_month_minus_8 <- most_recent_month %>%
  mutate(lag8 = most_recent_month - months(8)) %>%
  select(-most_recent_month)


most_recent_spend_minus_8 <- left_join(monthly_spend_by_mbr, 
                                       most_recent_month_minus_8, 
                                       by = "scene_mbr_key") %>%
  filter(yr_month == lag8) %>%
  mutate(lag_8_spending = most_recent_spending)%>%
  select(-yr_month, -most_recent_spending, -lag8)

master_table <- left_join(master_table, 
                          most_recent_spend_minus_8,
                          by = "scene_mbr_key")

# This table contains the five lagged month indicator
most_recent_month_minus_9 <- most_recent_month %>%
  mutate(lag9 = most_recent_month - months(9)) %>%
  select(-most_recent_month)


most_recent_spend_minus_9 <- left_join(monthly_spend_by_mbr, 
                                       most_recent_month_minus_9, 
                                       by = "scene_mbr_key") %>%
  filter(yr_month == lag9) %>%
  mutate(lag_9_spending = most_recent_spending)%>%
  select(-yr_month, -most_recent_spending, -lag9)

master_table <- left_join(master_table, 
                          most_recent_spend_minus_9,
                          by = "scene_mbr_key")

# This table contains the five lagged month indicator
most_recent_month_minus_10 <- most_recent_month %>%
  mutate(lag10 = most_recent_month - months(10)) %>%
  select(-most_recent_month)


most_recent_spend_minus_10 <- left_join(monthly_spend_by_mbr, 
                                       most_recent_month_minus_10, 
                                       by = "scene_mbr_key") %>%
  filter(yr_month == lag10) %>%
  mutate(lag_10_spending = most_recent_spending)%>%
  select(-yr_month, -most_recent_spending, -lag10)

master_table <- left_join(master_table, 
                          most_recent_spend_minus_10,
                          by = "scene_mbr_key")

# This table contains the five lagged month indicator
most_recent_month_minus_11 <- most_recent_month %>%
  mutate(lag11 = most_recent_month - months(11)) %>%
  select(-most_recent_month)


most_recent_spend_minus_11 <- left_join(monthly_spend_by_mbr, 
                                       most_recent_month_minus_11, 
                                       by = "scene_mbr_key") %>%
  filter(yr_month == lag11) %>%
  mutate(lag_11_spending = most_recent_spending)%>%
  select(-yr_month, -most_recent_spending, -lag11)

master_table <- left_join(master_table, 
                          most_recent_spend_minus_11,
                          by = "scene_mbr_key")

# This table contains the five lagged month indicator
most_recent_month_minus_12 <- most_recent_month %>%
  mutate(lag12 = most_recent_month - months(12)) %>%
  select(-most_recent_month)


most_recent_spend_minus_12 <- left_join(monthly_spend_by_mbr, 
                                       most_recent_month_minus_12, 
                                       by = "scene_mbr_key") %>%
  filter(yr_month == lag12) %>%
  mutate(lag_12_spending = most_recent_spending)%>%
  select(-yr_month, -most_recent_spending, -lag12)

master_table <- left_join(master_table, 
                          most_recent_spend_minus_12,
                          by = "scene_mbr_key")

na_filter <- is.na(master_table)
master_table[na_filter] <- 0

transaction_df <- master_table %>%
  ungroup()%>%
  select(-scene_mbr_key)%>%
  filter(yr_month =="2016-09-01")

scene_dim1 <- scene_mbr_dim %>% 
  dplyr::group_by(scene_mbr_key,scene_mbr_seq_num) %>%
  dplyr::arrange(desc(scene_mbr_seq_num))%>%
  dplyr::ungroup() 
scene_dim2 <- scene_mbr_dim %>% 
  dplyr::group_by(scene_mbr_key) %>%
  dplyr::arrange(desc(scene_mbr_seq_num)) %>%
  dplyr::summarise(scene_mbr_seq_num = max(scene_mbr_seq_num))
#an alternative to distinct function to delete duplicate entries  
scene_mbr_dim <- dplyr::inner_join(scene_dim2,scene_dim1)



fitControl <- trainControl(## 10-fold CV
  method = "repeatedcv",
  number = 10)

n = nrow(transaction_df)
trainIndex = sample(1:n, size = round(0.7*n), replace=FALSE)
train = transaction_df[trainIndex ,]
test = transaction_df[-trainIndex ,]



gbmFit1 <- train(most_recent_spending ~ ., data = train, 
                 method = "gbm", 
                 trControl = fitControl,
                 ## This last option is actually one
                 ## for gbm() that passes through
                 verbose = TRUE)

pred <- predict(gbmFit1, test)

postResample(pred = pred, obs = test$most_recent_spending)

pe <- abs(test$most_recent_spending-pred)/test$most_recent_spending

pe <- pe[!is.infinite(pe)]
mean(pe)

h2o.init()
master_table_h2o <- as.h2o(transaction_df)

splits <- h2o.splitFrame(master_table_h2o, 0.8)
train <- splits[[1]]
test <- splits[[2]]

# Tell h2o which are response variables and which are features
y <- "most_recent_spending"  
x <- names[names(scene_data)!=y]

m_rf_default <- h2o.randomForest(x, y, train, nfolds = 3, model_id = "RF_defaults")

# Show the performance on the test set
h2o.performance(m_rf_default, test)

m_gbm_default <- h2o.gbm(x, y, train, nfolds = 3, model_id = "GBM_defaults")

h2o.performance(m_gbm_default, test)

m_dl_default <- h2o.deeplearning(x, y, train, nfolds = 3, model_id = "GBM_defaults")

h2o.performance(m_dl_default, test)


