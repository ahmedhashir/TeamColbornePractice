rm(list=ls())
library(tidyverse)
library(data.table)
library(sparklyr)
library(rsparkling)
library(h2o)

# #Spark script
# config <- spark_config()
# config$'sparklyr.shell.executor-memory' <- "30g"
# config$'sparklyr.shell.driver-memory' <- "10g"
# # config$spark.yarn.am.memory <- "15g"
# 
# # Full list of config options: https://spark.apache.org/docs/2.0.1/running-on-yarn.html
# # 
# start<-Sys.time()
# 
# sc <- spark_connect(master = "yarn-client", spark_home = "/usr/hdp/current/spark2-client/", config=config)
# 
# in_path_spark = 'hdfs:///user/hpc3552/scene-csv/sample0003/clean/'
# 
# scene_mbr_dim <- spark_read_csv(sc, name='scene_mbr_dim', path=paste(in_path_spark, 'scene_mbr_dim.csv', sep=""), header = TRUE, delimiter = ",")
# 
# scene_mbr_acct_dim <- spark_read_csv(sc, name='scene_mbr_acct_dim', path=paste(in_path_spark, 'scene_mbr_acct_dim.csv', sep=""), header = TRUE, delimiter = ",")
# 
# scene_pt_fact <- spark_read_csv(sc, name='scene_pt_fact', path=paste(in_path_spark, 'scene_pt_fact.csv', sep=""), header = TRUE, delimiter = ",")


#R script
in_path_r = '/global/project/queens-mma/scene-csv/sample03/clean/'

#Read the regular R files for now
scene_mbr_dim <-fread(paste(in_path_r, 'scene_mbr_dim.csv', sep=""), sep=",")
scene_mbr_acct_dim <-fread(paste(in_path_r, 'scene_mbr_acct_dim.csv', sep=""), sep=",")
scene_pt_fact <-fread(paste(in_path_r, 'scene_pt_fact.csv', sep=""), sep=",")

scene_pt_fact_filter <- scene_pt_fact %>%
  filter(txn_amt <= 10000)

# This table has the most recent month that a customer spent something
most_recent_month <- scene_pt_fact_filter %>%
  mutate(yr_month = as.Date(time_lvl_st_dt)) %>%
  group_by(scene_mbr_key) %>%
  summarise(most_recent_month = max(yr_month))

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