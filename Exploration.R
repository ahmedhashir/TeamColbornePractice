library(tidyverse)
library(lubridate)

#change for git
#test2

#Set folder path
in_path = '/global/project/queens-mma/scene-csv/sample03/clean/'

#First dataset
scene_mbr_dim <- read_csv(paste(in_path, 'scene_mbr_dim.csv', sep=""))

#Dimensions of the data
dim(scene_mbr_dim)

#Column names
colnames(scene_mbr_dim)

#Structure
str(scene_mbr_dim)

#Summary:
summary(scene_mbr_dim)

#First 20 rows
head(scene_mbr_dim, n=20)

#=============================Member Entry Sequence (scene_mbr_seq_num)=========#

# This variable is not properly defined in the metadata. From the dataset, it appears that this number represents a unique entry for each customer and is used for customer record history. Whenever a member is entered in a database, s/he is assigned scene_mbr_seq_num as ‘1’. At the next change, this variable is changed to ‘2’ and so on. Thus, ‘1’ represents the first entry for the customer.

#Summary of the entry sequence:
summary(scene_mbr_dim$scene_mbr_seq_num)

#This shows the number of customers on each entry sequence
group_by(scene_mbr_dim, scene_mbr_seq_num)%>%
  summarise(count=n(), percentage = n()/nrow(scene_mbr_dim))%>%
  arrange(desc(count))

# ploting seq number <= 10 as the data is extremly skewed
scene_mbr_dim %>%
  filter(scene_mbr_seq_num <= 10) %>%
  ggplot(aes(x=scene_mbr_seq_num)) +
  geom_histogram(bins = 10)

# Because of this definition of sequence number, 50% of the records are ‘1’, while one third is ‘2’. Higher sequence numbers exponentially drop off. Sequence numbers can be used in combination with next parameters to understand the trends in the number of new customers and their renewal timelines.

#================ Effective Timestamps (eff_from_tmstamp, eff_to_tmstamp)===========#
# These timestamps represent the time period for which each sequence number is applicable.

scene_mbr_dim %>%
  filter(scene_mbr_seq_num==1) %>%
  ggplot(aes(x=eff_from_tmstamp)) +
  geom_histogram(bins = 77)

# To see all the new customers,i.e, sequence number ‘1’, there is huge sprike in March, 2010, with over 2000 entries. This must be the first time when the database was created. (bins=77 corresponds to the number of months in the database)

scene_mbr_dim %>%
  filter(scene_mbr_seq_num==1 & eff_from_tmstamp>ymd(20100305)) %>%
  ggplot(aes(x=eff_from_tmstamp)) +
  geom_histogram(bins = 77)

#Removing these initial entries and plotting again shows that new customers might have declined since 2014. (However, later analysis suggest possibility of database lag. See section on ‘scene_mbr_acct_dim’ for more details.) During the year, there appears to be cyclicality as there seems to be spikes around summer months.

# Plotting of data with sequence numbers such as ‘2’ and ‘3’ shows when the entries were updated.

scene_mbr_dim %>%
  filter(scene_mbr_seq_num==2) %>%
  ggplot(aes(x=eff_from_tmstamp))+
  geom_histogram(bins = 77)

#In plot of sequence number ‘2’ there are two spikes in March, 2011 and March, 2012, suggesting that the records created in March, 2010 might have been updated in bulk after 1 and 2 years respectively. There is also a spike toward the end of 2014, suggesting another bulk data ingestion.

scene_mbr_dim %>%
  filter(scene_mbr_seq_num==3) %>%
  ggplot(aes(x=eff_from_tmstamp)) +
  geom_histogram(bins = 77)


# For all subsequent sequence numbers, there is spike around March 2012 indicating that there were updates in 2012, but subsequently there have been only small number of renewals.
# 
# To find if the accounts have been renewed regularly, maximum of the “effective to timestamp” is analysed.

scene_mbr_dim %>%
  group_by(scene_mbr_acct_key) %>%
  summarise(EndDate=max(eff_to_tmstamp)) %>%
  ggplot(aes(x=EndDate)) +
  geom_histogram(bins = 77)

EndDateTbl <- scene_mbr_dim %>%
  group_by(scene_mbr_acct_key) %>%
  summarise(EndDate=max(eff_to_tmstamp))

scene_mbr_dim %>%
  group_by(scene_mbr_acct_key) %>%
  summarise(EndDate=max(eff_to_tmstamp)) %>%
  filter(EndDate>ymd(20160101))

group_by(EndDateTbl, year(EndDate))%>%
  summarise(count=n(),percentage = n()/nrow(EndDateTbl))

# 35% of the customers were never renewed after their expiry in 2012. Also, the proportion of customers effective after January 1, 2016 was only around 11%.

scene_mbr_dim %>%
  mutate(date_diff=eff_to_tmstamp-eff_from_tmstamp) %>%
  select(date_diff) %>%
  ggplot(aes(x=date_diff/24)) +
  geom_histogram()

# Each sequence number can be effective for any range of values. However, the typical values are 1 year and two years.

#=================== Birth Date (brth_dt)=================================#
# Birthday of the SCENE member, year only.

summary(scene_mbr_dim$brth_dt)

ggplot(data = scene_mbr_dim, mapping = aes(x=brth_dt))+
  geom_bar()

# The distribution of birth years is positively skewed with peak around 1990.
# 
# Postal Code (psnl_post_cd)
# The postal code of the address. First 3 characters only.

group_by(scene_mbr_dim, scene_mbr_acct_key, psnl_post_cd)%>%
  summarise(count=n())%>%
  group_by(psnl_post_cd)%>%
  summarise(count1=n())%>%
  arrange(desc(count1))

postal_codes<-read_csv('/global/project/queens-mma/ca_postal_codes.csv')

# renaming column
colnames(postal_codes)[1]<-"PostalCode" 
colnames(postal_codes)[2]<-"PlaceName" 

#joining with main file 
scene_mbr_dim<-
  left_join(scene_mbr_dim, postal_codes, by=c("psnl_post_cd"="PostalCode")) 

# install.packages('ggmap') 
#library(ggmap)
# can_map <- get_map(location="Canada", zoom=3, maptype="terrain")

# ggmap(can_map,extent="normal") +
#  geom_point(data = scene_mbr_dim, aes(x = Longitude, y = Latitude, alpha = 0.3)) + scale_size_continuous(range = c(1,6))

library(maps)

can_map <- map_data("world", "Canada")

ggplot() + 
  geom_polygon(data = can_map, aes(long, lat, group = group), fill = "white", colour = "grey") + 
  geom_point(data = scene_mbr_dim, aes(x = Longitude, y = Latitude, alpha = 0.1), colour = "blue") + 
  scale_size_continuous(range = c(1,6))

# Distribution of scene members seem generally consistent to the Canadian population. One member, however, seems to have H0H (postal code for Santa Claus, no kidding!) as address. This could be a mistake or some interesting special account??

Santa_Claus <- scene_mbr_dim %>%
  filter(psnl_post_cd == "H0H")

# Looking at the account residing in postal code H0H, this is some 38-old French speaking dude (or a fictious test account maybe?) who prefers to watch movies at ‘Cineplex Odeon Place LaSalle’ (Montreal). Look for member key ‘2315422’ if interested…

# Personal Province State Code (psnl_prov_state_cd)
# The state/province where the customer resides.

summary(as.factor(scene_mbr_dim$psnl_prov_state_cd))

ggplot(scene_mbr_dim, aes(x = factor(psnl_prov_state_cd))) +
  geom_bar()

# Ontario members accounts for over half of the member base. Here’s a pie chart, just for fun.

ggplot(scene_mbr_dim, aes(x = factor(1), fill = factor(psnl_prov_state_cd))) +
  geom_bar(width = 1)+ 
  coord_polar(theta = "y")

# City (psnl_city)
# The city of the address.

length(unique(scene_mbr_dim$psnl_city))

# There are 842 unique cities represented in this sample.
# 
# Number of customers per city:

scene_mbr_dim %>%
  group_by(psnl_city) %>%
  summarise(count= n(), percentage = n() / nrow(scene_mbr_dim)) %>%
  arrange(desc(count))

# Approximately 9% of the customers are from Toronto. Additional 8% customers are from Mississauga, Brampton, and Scarorough
# 
# Suspended Flag (suspended_f)
# Indicates whether the customer’s login has been suspended or not.

scene_mbr_dim %>%
  group_by(suspended_f) %>%
  summarise(count= n(), percentage = n() / nrow(scene_mbr_dim)) %>%
  arrange(desc(count))

# 4% customers have been suspended
# 
# Gender (gndr_desc)

scene_mbr_dim %>%
  group_by(gndr_desc) %>%
  summarise(count= n(), percentage = n() / nrow(scene_mbr_dim)) %>%
  arrange(desc(count))

group_by(scene_mbr_dim, psnl_prov_state_cd)%>%
  summarise(count= n())%>%
  left_join(scene_mbr_dim)%>%
  ggplot() + geom_bar(mapping=aes(x=psnl_prov_state_cd, fill = gndr_desc), position="fill")

group_by(scene_mbr_dim, brth_dt)%>%
  summarise(count= n())%>%
  left_join(scene_mbr_dim)%>%
  ggplot() + geom_bar(mapping=aes(x=brth_dt, fill = gndr_desc), position="fill")

# 55% of the members in the dataset are females. The ratio of female members are slightly higher than male members in most of Canadian Provinces. Yonger member base tend to have more male representation than do older members.
# 
# Preferred Location

group_by(scene_mbr_dim, prefrd_loctn_desc)%>%
  summarise(count=n_distinct(scene_mbr_key),percentage = n_distinct(scene_mbr_key) / n_distinct(scene_mbr_dim$scene_mbr_key))%>%
  arrange(desc(count))

scene_mbr_dim%>%
  filter(prefrd_loctn_desc!='Unknown')%>%
  group_by(psnl_city, prefrd_loctn_desc)%>%
  summarise(count=n_distinct(scene_mbr_acct_key))%>%
  arrange(desc(count))

# More than half of the data is unavailable. Scotiabank Theatre Chinook and SilverCity Yorkdale are rated the highest among the preferred theatres. If we had the exact address of the theatres, it can be combined with the postal codes of the customers to derive more insights.
# 
# Email Preference (email_prefnc_desc)
# Preferred format for emails

group_by(scene_mbr_dim, email_prefnc_desc)%>%
  summarise(count=n(), percentage = n()/nrow(scene_mbr_dim))%>%
  arrange(desc(count))

# 78% customers prefer HTML emails while 5% prefer plain text. 17% of preferences are unknown.
# 
# Education Level

group_by(scene_mbr_dim, ed_lvl_desc) %>%
  summarise(count=n(), percentage = n()/nrow(scene_mbr_dim))%>%
  arrange(desc(count))

group_by(scene_mbr_dim, psnl_prov_state_cd)%>%
  summarise(count= n())%>%
  left_join(scene_mbr_dim)%>%
  filter(ed_lvl_desc != "Unknown") %>%
  ggplot() + geom_bar(mapping=aes(x=psnl_prov_state_cd, fill = ed_lvl_desc), position="fill")

# 70% of the data is missing. Educational mix among members across provinces seem different.
# 
# Preferred Show Time

group_by(scene_mbr_dim, prefrd_show_tm_desc)%>%
  summarise(count=n(), percentage = n()/nrow(scene_mbr_dim))%>%
  arrange(desc(count))

group_by(scene_mbr_dim, psnl_city)%>%
  summarise(count= n())%>%
  arrange(desc(count))%>%
  filter(count>100)%>%
  left_join(scene_mbr_dim)%>%
  filter(prefrd_show_tm_desc!='Unknown')%>%
  ggplot()+
  geom_bar(mapping=aes(x=psnl_city, fill = prefrd_show_tm_desc), position="fill")

group_by(scene_mbr_dim, brth_dt)%>%
  summarise(count= n())%>%
  arrange(desc(count))%>%
  filter(count>100)%>%
  left_join(scene_mbr_dim)%>%
  filter(prefrd_show_tm_desc!='Unknown')%>%
  ggplot()+
  geom_bar(mapping=aes(x=brth_dt, fill = prefrd_show_tm_desc), position="fill")

# 58% of the data is missing. Of the people who have responded, 7pm seems to be the preferred movie time (39%). Pereferred show time seem different across cities and age groups. As expected, yonger members tend to prefer later shows.
# 
# Number of people in Household (num_of_hh_pple_desc)

group_by(scene_mbr_dim, num_of_hh_pple_desc)%>%
  summarise(count=n(), percentage = n()/nrow(scene_mbr_dim))%>%
  arrange(desc(count))

scene_mbr_dim%>%
  filter(prefrd_show_tm_desc!="Unknown")%>%
  ggplot()+
  geom_bar(mapping=aes(x=num_of_hh_pple_desc, fill = prefrd_show_tm_desc), position="fill")

# 57% of the data is missing. Of the people who have responded, 35% are single person households. There is no significant variation in the preferred timing based on the number of people in the household. However, this variable can be used for classification as it can indicate the number of kids in the household and children can be targetted with specific genre. It can also indicate potential profit opportunity size.
# 
# Movie Going Frequency (movie_gng_frq_ref_desc)

group_by(scene_mbr_dim, movie_gng_frq_ref_desc)%>%
  summarise(count=n(),percentage = n()/nrow(scene_mbr_dim))%>%
  arrange(desc(count))

movie_freq <- c("<2","3-6","7-10","11-20","20+")

scene_mbr_dim$movie_gng_frq_ref_desc<-
  factor(scene_mbr_dim$movie_gng_frq_ref_desc,levels=movie_freq, ordered=TRUE)

scene_mbr_dim%>%
  filter(movie_gng_frq_ref_desc !="NA")%>%
  ggplot()+
  geom_bar(mapping=aes(x=brth_dt, fill = movie_gng_frq_ref_desc), position="fill")

# 57% data missing. Customers are evenly spread over the frequency ranges 20+, 7-10, 11-20 and 3-6. As expected, younger members tend to watch more movies.
# 
# Marital Status (mrtl_stat_desc)

group_by(scene_mbr_dim, mrtl_stat_desc)%>%
  summarise(count=n(), percentage = n()/nrow(scene_mbr_dim))%>%
  arrange(desc(count))

scene_mbr_dim%>%
  filter(mrtl_stat_desc!='Unknown'&movie_gng_frq_ref_desc!='Unknown')%>%
  ggplot()+
  geom_bar(mapping=aes(x=mrtl_stat_desc, fill = movie_gng_frq_ref_desc), position="fill")

# 37% data missing, 35% singles and 26% married or common law. Married or common in law members tend to watch less movies compare to other groups.
# 
# Language (lang_desc)
# Preferred language

group_by(scene_mbr_dim, lang_desc)%>%
  summarise(count=n(),percentage = n()/nrow(scene_mbr_dim))%>%
  arrange(desc(count))

group_by(scene_mbr_dim, lang_desc)%>%
  summarise(count= n())%>%
  arrange(desc(count))%>%
  left_join(scene_mbr_dim)%>%
  ggplot()+
  geom_bar(mapping=aes(x=lang_desc, fill = psnl_prov_state_cd), position="fill")

# Over 90% of the customers have English as the preferred language. As expected, vast majority of Francophone members are from Quebect.
# 
# Scene Activity Status (scene_acty_stat)
# 0 = Unconfirmed - Members are Unconfirmed by default. These members have not had points expired, but they can be considered for points expiry. 1 = Confirmed Active - Members that are Confirmed Active are not to have points expired. 2 = Dormant - Members that are Dormant have had their points expired

group_by(scene_mbr_dim, scene_acty_stat)%>%
  summarise(count=n(),percentage = n()/nrow(scene_mbr_dim))%>%
  arrange(desc(count))

# 48% of the data is missing and 49% data is “Unconfirmed”.
# 
# Table: scene_member_acct_dim
# This table holds data on member accounts. Each member (from the above scene_mbr_dim table) belongs to a member account. Think of it like a family: there would be one account, but many members belonging to that account.

scene_mbr_acct_dim <- read_csv(paste(in_path, 'scene_mbr_acct_dim.csv', sep=""))

# Here are the details of the table:


dim(scene_mbr_acct_dim)

colnames(scene_mbr_acct_dim)

str(scene_mbr_acct_dim)

summary(scene_mbr_acct_dim)

# Here are the first 20 rows of the data:

head(scene_mbr_acct_dim, n=20)

# Enrollment Status Code (enrollment_stat_cd)
# Possible values: - E = Enrolled - U = Unenrolled - P = Pending - C = Canceled

group_by(scene_mbr_acct_dim, enrollment_stat_cd)%>%
  summarise(count=n(),percentage = n()/nrow(scene_mbr_acct_dim))%>%
  arrange(desc(count))

# In this sample dataset, 5,489 (98%) enrolled and 84(2%) unerolled.
# 
# cncl_dt
# Cancel date. 98% of the field is ‘null’.

scene_mbr_acct_dim$cncl_dt <- ymd_hms(scene_mbr_acct_dim$cncl_dt)

group_by(scene_mbr_acct_dim, cncl_dt)%>%
  summarise(count=n(),percentage = n()/nrow(scene_mbr_acct_dim))%>%
  arrange(desc(count))

scene_mbr_acct_dim%>%
  filter(!is.na(cncl_dt))%>%
  ggplot(aes(x=cncl_dt))+
  geom_histogram()

# 98% of the field is ‘null’, which is expected as most people would not call in to cancel a loyalty account even if they no longer use it. The number of cancellation seems to be increasing over time. However, this could be misleading as we might see the number of accounts growing over time as well. We should look at cancellation in conjunction with the number of accounts over time. Comparing the number of accounts having a cancel date with enrollment status above, ‘U’ and ‘C’ seem to be equivalent.
# 
# Enrollment Date (scene_src_enrollment_dt)
# Original member enrollment Date sourced from Maritz.

scene_mbr_acct_dim%>%
  ggplot(aes(x=scene_src_enrollment_dt))+
  geom_histogram()

# The number of enrollment seems to have significantly dropped since 2014, which seem unintuitive. A possibility that could exlain what we see here is the time lag between when an account is enrolled vs. when the data enters into the database we are using. See next point below.
# 
# acct_eff_from_tmstamp
# Date from which that specific record became effective.

group_by(scene_mbr_acct_dim, as.Date(acct_eff_from_tmstamp))%>%
  summarise(count=n(),percentage = n()/nrow(scene_mbr_acct_dim))%>%
  arrange(desc(count))

scene_mbr_acct_dim%>%
  ggplot(aes(x=acct_eff_from_tmstamp))+
  geom_histogram()

ggplot(data=scene_mbr_acct_dim) +
  geom_point(mapping = aes(x=as.Date(scene_src_enrollment_dt),y= date_lag))

# Approximatly 40% of accounts in the tiny dataset have an effective date of 2010-03-03. This seems to be the date where enrollment data (from Maritz) started to be ingested into a new database. The range of date lag between the original enrollment date and account effective date decreases linearly as time progresses. This further confirms the theory above–newly enrolled account might experience a range of lags before they are reflected in this database.

# Table: scene_pt_fact
# This table holds the actual transactions. For your convenience, it has already been joined with two other tables, iwd_time and scene_tp_pt_fact, both described below.

scene_pt_fact <- read_csv(paste(in_path, 'scene_pt_fact.csv', sep=""))

# Here are the details of the table:

dim(scene_pt_fact)

colnames(scene_pt_fact)

str(scene_pt_fact)

summary(scene_pt_fact)

# Here are the first 20 rows of the data:

head(scene_pt_fact, n=20)

# Points (pt) & Transaction Amount (txn_amt)
# These are the number of points earned or deducted for each transaction. They may be: 1. Linked to a financial transaction, in which case the transaction column will have dollar value. 2. Linked to an event (such as new account, referral bonus, redemptions, etc.), in which case the transaction column will be  NULL.
# 
# High level summary per customer:

summary<-
  group_by(scene_pt_fact, scene_mbr_acct_key)%>%
  summarise(number_of_txns=n(),
            total_points= sum(pt),
            total_txn_amt= sum(as.numeric(txn_amt),na.rm=TRUE))%>%
  arrange(desc(total_points))

summary%>%
  ggplot(aes(x=number_of_txns))+
  geom_histogram(bins = 100)

summary%>%
  ggplot(aes(x=total_points))+
  geom_histogram(bins = 100)

summary%>%
  ggplot(aes(x=total_txn_amt))+
  geom_histogram(bins = 100)  

# There are 328,357 customers in the sample, which have a wide range of transactions and points. The number of transactions show negative exponential distribution. The number of points net of redemptions is also extremely skewed to the left. We need to further analyze transaction inflow and outflow instead of solely looking at simple aggregation of points per member.
# 
# Some high level count per transaction types

group_by(scene_pt_fact, txn_tp_1)%>%
  summarise(count=n(),
            percent = count/length(scene_pt_fact$txn_tp_1)*100,
            avg_pts_txn = mean(as.numeric(pt), na.rm=TRUE),
            avg_dollar_txn = mean(as.numeric(txn_amt), na.rm=TRUE))%>%
  arrange(desc(count))

group_by(scene_pt_fact, txn_tp_2)%>%
  summarise(count=n(),
            percent = count/length(scene_pt_fact$txn_tp_2)*100,
            avg_pts_txn = mean(as.numeric(pt), na.rm=TRUE),
            avg_dollar_txn = mean(as.numeric(txn_amt), na.rm=TRUE))%>%
  arrange(desc(count))

group_by(scene_pt_fact, txn_tp_3)%>%
  summarise(count=n(),
            percent = count/length(scene_pt_fact$txn_tp_3)*100,
            avg_pts_txn = mean(as.numeric(pt), na.rm=TRUE),
            avg_dollar_txn = mean(as.numeric(txn_amt), na.rm=TRUE))%>%
  arrange(desc(count))

# Over 91% of transaction involves issuance(collection) of points, vs. only 9% of transactions were related to redemptions. So averagely speaking, members need to collect points over 10 purchases/activities before they redeem pts for once.
# 
# 95 percent of point transactions are tied to regular purchases and redemptions. At least 45% of activities (purchases and redemptions) happen outside of cineplex theaters (example: purchase on scene debit/credit cards outside of Cineplex).
# 
# Points issued

scene_pt_fact%>%
  filter(txn_tp_1=="issuance")%>%
  ggplot(mapping = aes(x=pt))+
  geom_histogram(bins=500) + scale_x_continuous(limits = c(0, 5000))

group_by(scene_pt_fact, txn_tp_2)%>%
  filter(txn_tp_1=="issuance")%>%
  summarise(count=n(), total_pts = sum(as.numeric(pt), na.rm=TRUE))%>%
  arrange(desc(count))

group_by(scene_pt_fact, txn_tp_3)%>%
  filter(txn_tp_1=="issuance")%>%
  summarise(count=n(), total_pts = sum(as.numeric(pt), na.rm=TRUE))%>%
  arrange(desc(count))

# 94% of issuances and 88% of points issued were associated with regular transactions. 50% of points issued happened outside of cineplex theaters.

scene_pt_fact %>%
  filter(txn_tp_1=="issuance")%>%
  ggplot(aes(x=as.numeric(txn_amt, na.rm=TRUE), y=as.numeric(pt, na.rm=TRUE), group=txn_tp_2, colour=txn_tp_2)) + geom_point()

# While enrollment and promotional bonuses are typically not involving dollar transactions, regular points seems to follow a few specific earn rates, which likely represent earn rates at cineplex vs. non-cineplex locations. Note that sometimes, dollar spend triggers negative points, these could be redemptions at the same time of purchases, or potentially data base errors.
# 
# Redemptions

scene_pt_fact%>%
  filter(txn_tp_1=="redemption")%>%
  ggplot(mapping = aes(x=pt))+
  geom_histogram(bins=500) + scale_x_continuous(limits = c(-5000, 0))

group_by(scene_pt_fact, txn_tp_3)%>%
  filter(txn_tp_1=="redemption")%>%
  summarise(count=n(), total_pts = sum(as.numeric(pt), na.rm=TRUE))%>%
  arrange(desc(count))

# The number of points redeemed tends to be multipliers of 1000, which is worth a general admission ticket. 98% of points were redeemed at cineplex theaters.
# 
# Timelines

group_by(scene_pt_fact, time_lvl_end_dt, txn_tp_1)%>%
  summarise(mth_pts = sum(as.numeric(pt, na.rm=TRUE)))%>%
  ggplot() + geom_line(aes(x=time_lvl_end_dt, y=mth_pts, group=txn_tp_1, colour=txn_tp_1))

group_by(scene_pt_fact, time_lvl_end_dt, txn_tp_2)%>%
  filter(txn_tp_1=="issuance")%>%
  summarise(mth_pts = sum(as.numeric(pt, na.rm=TRUE)))%>%
  ggplot() + geom_line(aes(x=time_lvl_end_dt, y=mth_pts, group=txn_tp_2, colour=txn_tp_2))

group_by(scene_pt_fact, time_lvl_end_dt, txn_tp_3)%>%
  filter(txn_tp_1=="issuance")%>%
  summarise(mth_pts = sum(as.numeric(pt, na.rm=TRUE)))%>%
  ggplot() + geom_line(aes(x=time_lvl_end_dt, y=mth_pts, group=txn_tp_3, colour=txn_tp_3))

# More and more points are being issued for day-to-day transactions while enrollment bonues seems to have decreased post-2010. Non-cin transactions look perfectly cyclical on a yearly basis. Although issuance at cineplext vs. other places seem to follow the same overall trend, high & lows don’t seem to happen in the same months.

scene_pt_fact$mo_clndr_code<-
  factor(scene_pt_fact$mo_clndr_code,levels=month.name, ordered=TRUE)

group_by(scene_pt_fact, mo_clndr_code, txn_tp_3)%>%
  filter(txn_tp_1=="issuance")%>%
  summarise(mth_pts = sum(as.numeric(pt, na.rm=TRUE)))%>%
  ggplot() + geom_line(aes(x=mo_clndr_code, y=mth_pts, group=txn_tp_3, colour=txn_tp_3))

# A quick view at the aggregate month level, we see that cineplex pts issuane peaks in the summer, while non-cin points peaks in December as well.
# 
# Table: scene_pt_tp_dim
# This table provides descriptive information, such as point types, merchant names and transaction source, to transactions recorded in the ‘scene_pt_fact’ table.
# 
# This table does not provide much value by itself. It has already been joined to the ‘scene_pt_fact’ table, and thus will probably not be useful on its own.

scene_pt_tp_dim <- read_csv(paste(in_path, 'scene_pt_tp_dim.csv', sep=""))

# Here are the details of the table:

dim(scene_pt_tp_dim)

colnames(scene_pt_tp_dim)

str(scene_pt_tp_dim)

summary(scene_pt_tp_dim)

# Here are the first 20 rows of the data:

head(scene_pt_tp_dim, n=20)

# Table: iwd_time
# This table lists dates and times. This table also does not provide much value by itself. It is mapped to the ‘scene_pt_fact’ table and provides time descriptions to the transactions recorded in that table.

iwd_time <- read_csv(paste(in_path, 'iwd_time.csv', sep=""))

# Here are the details of the table:

dim(iwd_time)

colnames(iwd_time)

str(iwd_time)

summary(iwd_time)

# Here are the first 20 rows of the data:

head(iwd_time, n=20)

# time level (time_lvl)

summary(as.factor(iwd_time$time_lvl))

# This field categorizes time_key into 4 levels: Day, Month, Quarter, Year
# 
# month (mo_clndr_code)
# This is the name of the calendar month.

summary(as.factor(iwd_time$mo_clndr_code))

# NOTE: * The scene_pt_fact table gives us time keys at the month level. So, for purpose of analysing point transactions, we only need to map the ‘mo_clndr_code’ field to the scene_pt_fact table. ‘time_lvl’ field can be used to select “Month” level keys only. * Although other fields from this table cannot be linked to point transactions, the following fields from the ‘iwd_time’ table provides some interesting facts.
# 
# anul_clndr_code
# This indicates the calendar year of the corresponding time key.

summary(iwd_time$anul_clndr_code)

# earliest calendar year stamp: 1994, latest calendar year stamp: 2021
# 
# anual_fncl_code
# This indicates the bank’s fiscal year of the corresponding time key.

summary(iwd_time$anul_fncl_code)

# earliest fiscal year stamp: 1994, latest fiscal year stamp: 2021
# 
# Check if calendar year always equal fiscal year, Returned False.

all(iwd_time$anul_clndr_code == iwd_time$anul_fncl_code)

iwd_time %>%
  filter(anul_clndr_code != anul_fncl_code) %>%
  select(mo_clndr_code) %>%
  group_by(mo_clndr_code) %>%
  count(mo_clndr_code)

# Looking at the month where calendar year differs from fiscal year, November seems to be the start of fiscal year at Scotiabank.
# 
# qtrly_clndr_code
# This field indicates the calendar quarter of the corresponding time_key.

summary(as.factor(iwd_time$qtrly_clndr_code))

iwd_time %>%
  filter(qtrly_clndr_code == "null") %>%
  select(time_lvl) %>%
  group_by(time_lvl) %>%
  count(time_lvl)

# 28 missing values found from quarter code, all for year level time keys.
# 
# qtrly_fncl_code
# This field indicates the fiscal quarter of the corresponding time_key.

summary(as.factor(iwd_time$qtrly_fncl_code))
