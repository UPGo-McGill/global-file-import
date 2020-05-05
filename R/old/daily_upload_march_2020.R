library(tidyverse)
library(strr)
library(upgo)
library(future)
plan(multiprocess)

year <- "2019_12"

load(paste0("output/", substr(year, 1, 4), "/daily_", year, ".Rdata"))
load(paste0("output/", substr(year, 1, 4), "/daily_inactive_", year, ".Rdata"))

daily_inactive <- 
  daily %>% 
  filter(status == "U") %>% 
  bind_rows(daily_inactive) %>% 
  arrange(property_ID, start_date)

daily <- 
  daily %>% 
  filter(status != "U") %>% 
  arrange(property_ID, start_date)

save(daily, file = paste0("output/", substr(year, 1, 4), "/daily_", year, 
                          ".Rdata"))
save(daily_inactive, file = paste0("output/", substr(year, 1, 4), 
                                   "/daily_inactive_", year, ".Rdata"))

host <-
  daily %>% 
  strr_expand() %>% 
  strr_host() %>% 
  strr_compress()

save(host, file = paste0("output/", substr(year, 1, 4), "/host_", year, 
                         ".Rdata"))

host_inactive <- 
  daily_inactive %>%
  strr_expand() %>%
  strr_host() %>% 
  strr_compress()

save(host_inactive, 
     file = paste0("output/", substr(year, 1, 4), "/host_inactive_", year, 
                   ".Rdata"))

### Uploads ####################################################################

upgo_connect(property = FALSE, daily_inactive = TRUE, host_inactive = TRUE)

year <- "2020_02"

load(paste0("output/", substr(year, 1, 4), "/daily_", year, ".Rdata"))
load(paste0("output/", substr(year, 1, 4), "/daily_inactive_", year, ".Rdata"))
load(paste0("output/", substr(year, 1, 4), "/host_", year, ".Rdata"))
load(paste0("output/", substr(year, 1, 4), "/host_inactive_", year, ".Rdata"))

daily_all %>% 
  filter(property_ID == !! daily[1,]$property_ID,
         start_date == !! daily[1,]$start_date) %>% 
  collect() %>% 
  nrow()

daily_inactive_all %>% 
  filter(property_ID == !! daily_inactive[1,]$property_ID,
         start_date == !! daily_inactive[1,]$start_date) %>% 
  collect() %>% 
  nrow()

host_all %>% 
  filter(host_ID == !! host[1,]$host_ID,
         start_date == !! host[1,]$start_date) %>% 
  collect() %>% 
  nrow()

host_inactive_all %>% 
  filter(host_ID == !! host_inactive[1,]$host_ID,
         start_date == !! host_inactive[1,]$start_date) %>% 
  collect() %>% 
  nrow()


Sys.time()
daily %>% 
  RPostgres::dbWriteTable(con, "daily", ., append = TRUE)
Sys.time()

daily_all %>% 
  filter(property_ID == !! daily[1,]$property_ID,
         start_date == !! daily[1,]$start_date) %>% 
  collect() %>% 
  nrow()

Sys.time()
daily_inactive %>% 
  RPostgres::dbWriteTable(con, "daily_inactive", ., append = TRUE)
Sys.time()

daily_inactive_all %>% 
  filter(property_ID == !! daily_inactive[1,]$property_ID,
         start_date == !! daily_inactive[1,]$start_date) %>% 
  collect() %>% 
  nrow()

Sys.time()
host %>% 
  RPostgres::dbWriteTable(con, "host", ., append = TRUE)
Sys.time()

host_all %>% 
  filter(host_ID == !! host[1,]$host_ID,
         start_date == !! host[1,]$start_date) %>% 
  collect() %>% 
  nrow()

Sys.time()
host_inactive %>% 
  RPostgres::dbWriteTable(con, "host_inactive", ., append = TRUE)
Sys.time()

host_inactive_all %>% 
  filter(host_ID == !! host_inactive[1,]$host_ID,
         start_date == !! host_inactive[1,]$start_date) %>% 
  collect() %>% 
  nrow()

upgo_disconnect()
