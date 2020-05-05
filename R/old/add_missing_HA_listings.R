library(tidyverse)
library(strr)
library(future)
plan(multiprocess)

error_2014 <- 
  read_csv("output/2014/error_2014.csv")

error_2015 <- 
  read_csv("output/2015/error_2015.csv")

error_2016 <- 
  read_csv("output/2016/error_2016.csv")

error_2017 <- 
  read_csv("output/2017/error_2017.csv")

error_2018 <- 
  read_csv("output/2018/error_2018.csv")


error_2018 %>% 
  filter(property_ID %in% property$property_ID)



## 2017 setup

add_2017 <- 
  error_2017 %>% 
  filter(property_ID %in% property$property_ID)

error_2017 <- 
  error_2017 %>% 
  filter(!property_ID %in% add_2017$property_ID)

if ((property %>% 
     filter(is.na(created) | is.na(scraped)) %>% 
     filter(property_ID %in% add_2017$property_ID) %>% 
     nrow()) > 0) {
  
  HA_created_NAs <- 
    property %>% 
    filter(is.na(created) | is.na(scraped)) %>% 
    filter(property_ID %in% add_2017$property_ID) %>% 
    pull(property_ID)
  
  property <-
    add_2017 %>% 
    filter(property_ID %in% HA_created_NAs, status != "U") %>% 
    group_by(property_ID) %>% 
    filter(date == min(date)) %>% 
    select(property_ID, date) %>% 
    left_join(property, .) %>% 
    mutate(created = if_else(!is.na(date), date, created)) %>% 
    select(-date)
  
  save(property, file = "output/property/property_2019_12.Rdata")
  
  rm(HA_created_NAs)
  
}

output_2017 <- 
  add_2017 %>% 
  strr_process_daily(property)




## 2018 setup

add_2018 <- 
  error_2018 %>% 
  filter(property_ID %in% property$property_ID)

error_2018 <- 
  error_2018 %>% 
  filter(!property_ID %in% add_2018$property_ID)

if ((property %>% 
     filter(is.na(created) | is.na(scraped)) %>% 
     filter(property_ID %in% add_2018$property_ID) %>% 
     nrow()) > 0) {
  
  HA_created_NAs <- 
    property %>% 
    filter(is.na(created) | is.na(scraped)) %>% 
    filter(property_ID %in% add_2018$property_ID) %>% 
    pull(property_ID)
  
  property <-
    add_2018 %>% 
    filter(property_ID %in% HA_created_NAs, status != "U") %>% 
    group_by(property_ID) %>% 
    filter(date == min(date)) %>% 
    select(property_ID, date) %>% 
    left_join(property, .) %>% 
    mutate(created = if_else(!is.na(date), date, created)) %>% 
    select(-date)
  
  save(property, file = "output/property/property_2019_12.Rdata")
  
  rm(HA_created_NAs)
  
}


## Process daily files

output_2018 <- 
  add_2018 %>% 
  strr_process_daily(property)


daily_2017_add <- 
  output_2017[[1]] %>% 
  strr_compress()

daily_inactive_2017_add <- 
  output_2017[[2]] %>% 
  strr_compress()

daily_2018_add <- 
  output_2018[[1]] %>% 
  strr_compress()

daily_inactive_2018_add <- 
  output_2018[[2]] %>% 
  strr_compress()


## Process host files

host_2017_add <- 
  output_2017[[1]] %>% 
  strr_host() %>% 
  strr_compress()

host_inactive_2017_add <- 
  output_2017[[2]] %>% 
  strr_host() %>% 
  strr_compress()

host_2018_add <- 
  output_2018[[1]] %>% 
  strr_host() %>% 
  strr_compress()

host_inactive_2018_add <- 
  output_2018[[2]] %>% 
  strr_host() %>% 
  strr_compress()


## Save temp

save(daily_2017_add, daily_2018_add, daily_inactive_2017_add, daily_inactive_2018_add,
     host_2017_add, host_2018_add, host_inactive_2017_add, host_inactive_2018_add,
     error_2017, error_2018, file = "MISSING_LISTINGS_REDO.Rdata")



## Integrate host files into main ones

host_to_add_2017 <- 
  host %>% 
  filter(host_ID %in% host_2017_add$host_ID) %>% 
  strr_expand() %>% 
  bind_rows(strr_expand(host_2017_add))

host_redo <-
  host_to_add_2017 %>% 
  group_by(host_ID, date, listing_type, housing) %>% 
  summarize(count = sum(count)) %>% 
  ungroup()

host_redo_compressed <- 
  host_redo %>% 
  strr_compress()

host <- 
  host %>% 
  filter(!host_ID %in% host_redo_compressed$host_ID) %>% 
  bind_rows(host_redo_compressed) %>% 
  arrange(host_ID, start_date)

save(host, file = "output/2017/host_2017.Rdata")

host_inactive_to_add_2017 <- 
  host_inactive %>% 
  filter(host_ID %in% host_inactive_2017_add$host_ID) %>% 
  strr_expand() %>% 
  bind_rows(strr_expand(host_inactive_2017_add))

host_inactive_redo <-
  host_inactive_to_add_2017 %>% 
  group_by(host_ID, date, listing_type, housing) %>% 
  summarize(count = sum(count)) %>% 
  ungroup()

host_inactive_redo_compressed <- 
  host_inactive_redo %>% 
  strr_compress()

host_inactive <- 
  host_inactive %>% 
  filter(!host_ID %in% host_inactive_redo_compressed$host_ID) %>% 
  bind_rows(host_inactive_redo_compressed) %>% 
  arrange(host_ID, start_date)

save(host_inactive, file = "output/2017/host_inactive_2017.Rdata")


host_to_add_2018 <- 
  host %>% 
  filter(host_ID %in% host_2018_add$host_ID) %>% 
  strr_expand() %>% 
  bind_rows(strr_expand(host_2018_add))

host_redo <-
  host_to_add_2018 %>% 
  group_by(host_ID, date, listing_type, housing) %>% 
  summarize(count = sum(count)) %>% 
  ungroup()

host_redo_compressed <- 
  host_redo %>% 
  strr_compress()

host <- 
  host %>% 
  filter(!host_ID %in% host_redo_compressed$host_ID) %>% 
  bind_rows(host_redo_compressed) %>% 
  arrange(host_ID, start_date)

save(host, file = "output/2018/host_2018.Rdata")

host_inactive_to_add_2018 <- 
  host_inactive %>% 
  filter(host_ID %in% host_inactive_2018_add$host_ID) %>% 
  strr_expand() %>% 
  bind_rows(strr_expand(host_inactive_2018_add))

host_inactive_redo <-
  host_inactive_to_add_2018 %>% 
  group_by(host_ID, date, listing_type, housing) %>% 
  summarize(count = sum(count)) %>% 
  ungroup()

host_inactive_redo_compressed <- 
  host_inactive_redo %>% 
  strr_compress()

host_inactive <- 
  host_inactive %>% 
  filter(!host_ID %in% host_inactive_redo_compressed$host_ID) %>% 
  bind_rows(host_inactive_redo_compressed) %>% 
  arrange(host_ID, start_date)

save(host_inactive, file = "output/2018/host_inactive_2018.Rdata")





## Append daily files to main ones

daily <- 
  bind_rows(daily, daily_2017_add) %>% 
  arrange(property_ID, start_date)

save(daily, file = "output/2017/daily_2017.Rdata")

rm(daily)

daily_inactive <- 
  bind_rows(daily_inactive, daily_inactive_2017_add) %>% 
  arrange(property_ID, start_date)

save(daily_inactive, file = "output/2017/daily_inactive_2017.Rdata")

rm(daily_inactive)

write_csv(error_2017, "output/2017/error_2017.csv")



daily_2018 <- 
  bind_rows(daily_2018, daily_2018_add) %>% 
  arrange(property_ID, start_date)

save(daily_2018, file = "output/2017/daily_2018.Rdata")

daily_inactive_2018 <- 
  bind_rows(daily_inactive_2018, daily_inactive_2018_add) %>% 
  arrange(property_ID, start_date)

save(daily_inactive_2018, file = "output/2018/daily_inactive_2018.Rdata")



## Upload

library(upgo)
upgo_connect()

Sys.time()
daily_2017_add %>% 
  RPostgres::dbWriteTable(con, "daily_new", ., append = TRUE)
Sys.time()

daily_new_all <- tbl(con, "daily_new")

daily_new_all %>% 
  filter(property_ID == !! daily_2017_add[1,]$property_ID,
         start_date == !! daily_2017_add[1,]$start_date) %>% 
  collect() %>% 
  nrow()

Sys.time()
daily_2018_add %>% 
  RPostgres::dbWriteTable(con, "daily_new", ., append = TRUE)
Sys.time()

daily_new_all <- tbl(con, "daily_new")

daily_new_all %>% 
  filter(property_ID == !! daily_2018_add[1,]$property_ID,
         start_date == !! daily_2018_add[1,]$start_date) %>% 
  collect() %>% 
  nrow()

Sys.time()
daily_inactive_2017_add %>% 
  RPostgres::dbWriteTable(con, "daily_new", ., append = TRUE)
Sys.time()

daily_new_all <- tbl(con, "daily_new")

daily_new_all %>% 
  filter(property_ID == !! daily_inactive_2017_add[1,]$property_ID,
         start_date == !! daily_inactive_2017_add[1,]$start_date) %>% 
  collect() %>% 
  nrow()

Sys.time()
daily_inactive_2018_add %>% 
  RPostgres::dbWriteTable(con, "daily_new", ., append = TRUE)
Sys.time()

daily_new_all <- tbl(con, "daily_new")

daily_new_all %>% 
  filter(property_ID == !! daily_inactive_2018_add[1,]$property_ID,
         start_date == !! daily_inactive_2018_add[1,]$start_date) %>% 
  collect() %>% 
  nrow()

