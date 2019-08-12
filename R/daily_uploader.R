#### UPLOAD COMPRESSED DAILY FILES #############################################

### Open connection ############################################################

library(tidyverse)
library(RPostgres)
library(data.table)

con <- RPostgres::dbConnect(
  RPostgres::Postgres(),
  dbname = "airdna")


### 2019 #######################################################################

load("output/2019/compressed_2019_01.Rdata")
load("output/2019/compressed_2019_02.Rdata")
load("output/2019/compressed_2019_03.Rdata")
load("output/2019/compressed_2019_04.Rdata")

RPostgres::dbWriteTable(con, "daily", compressed_2019_01, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2019_02, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2019_03, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2019_04, append = TRUE)

rm(compressed_2019_01, compressed_2019_02, compressed_2019_03,
   compressed_2019_04)


load("output/2019/compressed_2019_05.Rdata")
load("output/2019/compressed_2019_06.Rdata")
load("output/2019/compressed_2019_07.Rdata")
load("output/2019/compressed_2019_08.Rdata")
load("output/2019/compressed_2019_09.Rdata")

RPostgres::dbWriteTable(con, "daily", compressed_2019_05, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2019_06, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2019_07, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2019_08, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2019_09, append = TRUE)

rm(compressed_2019_05, compressed_2019_06, compressed_2019_07,
   compressed_2019_08, compressed_2019_09)


load("output/2019/compressed_2019_10.Rdata")
load("output/2019/compressed_2019_11.Rdata")
load("output/2019/compressed_2019_12.Rdata")
load("output/2019/compressed_2019_13.Rdata")
load("output/2019/compressed_2019_14.Rdata")

RPostgres::dbWriteTable(con, "daily", compressed_2019_10, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2019_11, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2019_12, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2019_13, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2019_14, append = TRUE)

rm(compressed_2019_10, compressed_2019_11, compressed_2019_12,
   compressed_2019_13, compressed_2019_14)


### 2018 #######################################################################

load("output/2018/compressed_2018_1.Rdata")
load("output/2018/compressed_2018_2.Rdata")
load("output/2018/compressed_2018_3.Rdata")
load("output/2018/compressed_2018_4.Rdata")

RPostgres::dbWriteTable(con, "daily", compressed_2018_1, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2018_2, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2018_3, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2018_4, append = TRUE)

rm(compressed_2018_1, compressed_2018_2, compressed_2018_3, compressed_2018_4)


load("output/2018/compressed_2018_5.Rdata")
load("output/2018/compressed_2018_6.Rdata")
load("output/2018/compressed_2018_7.Rdata")
load("output/2018/compressed_2018_8.Rdata")

RPostgres::dbWriteTable(con, "daily", compressed_2018_5, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2018_6, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2018_7, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2018_8, append = TRUE)

rm(compressed_2018_5, compressed_2018_6, compressed_2018_7, compressed_2018_8)


load("output/2018/compressed_2018_9.Rdata")
load("output/2018/compressed_2018_10.Rdata")
load("output/2018/compressed_2018_11.Rdata")
load("output/2018/compressed_2018_12.Rdata")

RPostgres::dbWriteTable(con, "daily", compressed_2018_9, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2018_10, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2018_11, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2018_12, append = TRUE)

rm(compressed_2018_9, compressed_2018_10, compressed_2018_11, 
   compressed_2018_12)


load("output/2018/compressed_2018_13.Rdata")
load("output/2018/compressed_2018_14.Rdata")
load("output/2018/compressed_2018_15.Rdata")
load("output/2018/compressed_2018_16.Rdata")

RPostgres::dbWriteTable(con, "daily", compressed_2018_13, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2018_14, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2018_15, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2018_16, append = TRUE)

rm(compressed_2018_13, compressed_2018_14, compressed_2018_15, 
   compressed_2018_16)


load("output/2018/compressed_2018_17.Rdata")
load("output/2018/compressed_2018_18.Rdata")
load("output/2018/compressed_2018_19.Rdata")
load("output/2018/compressed_2018_20.Rdata")

RPostgres::dbWriteTable(con, "daily", compressed_2018_17, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2018_18, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2018_19, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2018_20, append = TRUE)

rm(compressed_2018_17, compressed_2018_18, compressed_2018_19, 
   compressed_2018_20)


load("output/2018/compressed_2018_21.Rdata")
load("output/2018/compressed_2018_22.Rdata")
load("output/2018/compressed_2018_23.Rdata")
load("output/2018/compressed_2018_24.Rdata")
load("output/2018/compressed_2018_25.Rdata")

RPostgres::dbWriteTable(con, "daily", compressed_2018_21, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2018_22, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2018_23, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2018_24, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2018_25, append = TRUE)

rm(compressed_2018_21, compressed_2018_22, compressed_2018_23, 
   compressed_2018_24, compressed_2018_25)


### 2017 #######################################################################

load("output/2017/compressed_2017_1.Rdata")
load("output/2017/compressed_2017_2.Rdata")
load("output/2017/compressed_2017_3.Rdata")
load("output/2017/compressed_2017_4.Rdata")

RPostgres::dbWriteTable(con, "daily", compressed_2017_1, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2017_2, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2017_3, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2017_4, append = TRUE)

rm(compressed_2017_1, compressed_2017_2, compressed_2017_3, compressed_2017_4)


load("output/2017/compressed_2017_5.Rdata")
load("output/2017/compressed_2017_6.Rdata")
load("output/2017/compressed_2017_7.Rdata")
load("output/2017/compressed_2017_8.Rdata")

RPostgres::dbWriteTable(con, "daily", compressed_2017_5, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2017_6, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2017_7, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2017_8, append = TRUE)

rm(compressed_2017_5, compressed_2017_6, compressed_2017_7, compressed_2017_8)


load("output/2017/compressed_2017_9.Rdata")
load("output/2017/compressed_2017_10.Rdata")
load("output/2017/compressed_2017_11.Rdata")
load("output/2017/compressed_2017_12.Rdata")

RPostgres::dbWriteTable(con, "daily", compressed_2017_9, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2017_10, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2017_11, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2017_12, append = TRUE)

rm(compressed_2017_9, compressed_2017_10, compressed_2017_11, 
   compressed_2017_12)


load("output/2017/compressed_2017_13.Rdata")
load("output/2017/compressed_2017_14.Rdata")
load("output/2017/compressed_2017_15.Rdata")
load("output/2017/compressed_2017_16.Rdata")

RPostgres::dbWriteTable(con, "daily", compressed_2017_13, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2017_14, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2017_15, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2017_16, append = TRUE)

rm(compressed_2017_13, compressed_2017_14, compressed_2017_15, 
   compressed_2017_16)


load("output/2017/compressed_2017_17.Rdata")
load("output/2017/compressed_2017_18.Rdata")

RPostgres::dbWriteTable(con, "daily", compressed_2017_17, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2017_18, append = TRUE)

rm(compressed_2017_17, compressed_2017_18)


### 2016 #######################################################################

load("output/2016/compressed_2016_1.Rdata")
load("output/2016/compressed_2016_2.Rdata")
load("output/2016/compressed_2016_3.Rdata")
load("output/2016/compressed_2016_4.Rdata")

RPostgres::dbWriteTable(con, "daily", compressed_2016_1, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2016_2, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2016_3, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2016_4, append = TRUE)

rm(compressed_2016_1, compressed_2016_2, compressed_2016_3, compressed_2016_4)


load("output/2016/compressed_2016_5.Rdata")
load("output/2016/compressed_2016_6.Rdata")
load("output/2016/compressed_2016_7.Rdata")
load("output/2016/compressed_2016_8.Rdata")

RPostgres::dbWriteTable(con, "daily", compressed_2016_5, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2016_6, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2016_7, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2016_8, append = TRUE)

rm(compressed_2016_5, compressed_2016_6, compressed_2016_7, compressed_2016_8)


### 2015 #######################################################################

load("output/2015/compressed_2015_1.Rdata")
load("output/2015/compressed_2015_2.Rdata")
load("output/2015/compressed_2015_3.Rdata")

RPostgres::dbWriteTable(con, "daily", compressed_2015_1, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2015_2, append = TRUE)
RPostgres::dbWriteTable(con, "daily", compressed_2015_3, append = TRUE)

rm(compressed_2015_1, compressed_2015_2, compressed_2015_3)


### 2014 #######################################################################

load("output/2014/compressed_2014.Rdata")

RPostgres::dbWriteTable(con, "daily", compressed_2014, append = TRUE)

rm(compressed_2014)
