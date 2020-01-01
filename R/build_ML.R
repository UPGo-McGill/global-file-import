#### ML processing #############################################################

library(tidyverse)
library(strr)
library(upgo)
library(RPostgres)


### Initialize table ###########################################################

ML_drop <- 'DROP TABLE public.multi;'

ML_build <- 
  'CREATE TABLE public.multi
(
    "host_ID" text COLLATE pg_catalog."default",
    start_date date,
    end_date date,
    listing_type text COLLATE pg_catalog."default",
    housing boolean,
    active boolean,
    count integer,
    CONSTRAINT ML_primarykey PRIMARY KEY ("host_ID", start_date, listing_type, housing, active)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;'

ML_index_host_ID <-
  'CREATE INDEX "multi_index_host_ID"
  ON public.multi USING btree
  ("host_ID" COLLATE pg_catalog."default")
  TABLESPACE pg_default;'

ML_perm_dw <- 'GRANT ALL ON TABLE public.multi TO dwachsmuth;'
ML_perm_ro <-  'GRANT SELECT ON TABLE public.multi TO read_only;'

upgo_connect(property = FALSE, daily = FALSE, ML = FALSE)

dbExecute(con, ML_drop)
dbExecute(con, ML_build)
dbExecute(con, ML_index_host_ID)
dbExecute(con, ML_perm_dw)
dbExecute(con, ML_perm_ro)

rm(ML_drop, ML_drop2, ML_build, ML_index_host_ID, ML_perm_dw, ML_perm_ro)


### Process 2014 ###############################################################

year_month <- "2019_11"

load("output/2014/daily_2014.Rdata")
load(paste0("output/property/property_", year_month, ".Rdata"))


daily_2014 <-
  daily_2014 %>% 
  filter(property_ID %in% property$property_ID, status != "U") %>% 
  strr_expand(cores = 6)

ML_2014 <-
  daily_2014 %>% 
  mutate(active = if_else(date < created | date > scraped, FALSE, TRUE)) %>% 
  count(host_ID, date, listing_type, housing, active) %>% 
  ungroup() %>% 
  rename(count = n)

ML_2014 <-
  ML_2014 %>%
  strr_compress(cores = 6)

save(ML_2014, file = paste0("output/ML/ML_2014.Rdata"))

no_ML <- 
  ML_2014 %>% 
  filter(!(host_ID %in% property$host_ID)) %>% 
  rbind(no_ML)

save(no_ML, file = paste0("output/ML/no_ML_2014.Rdata"))

upgo_connect(property = FALSE, daily = FALSE, multi = FALSE)

ML_2014 %>% 
  filter(host_ID %in% property$host_ID) %>% 
  RPostgres::dbWriteTable(con, "multi", ., append = TRUE)


### Process 2015 ###############################################################

load("output/2015/daily_2015_1.Rdata")

ML_2015_1 <- 
  daily_2015_1 %>% 
  strr_expand(cores = 5) %>% 
  count(host_ID, date, listing_type) %>% 
  rename(count = n) 

rm(daily_2015_1)
save(ML_2015_1, file = "output/ML/ML_2015_1.Rdata")
rm(ML_2015_1)

load("output/2015/daily_2015_2.Rdata")

ML_2015_2 <- 
  daily_2015_2 %>% 
  strr_expand(cores = 5) %>% 
  count(host_ID, date, listing_type) %>% 
  rename(count = n) 

rm(daily_2015_2)
save(ML_2015_2, file = "output/ML/ML_2015_2.Rdata")
rm(ML_2015_2)

load("output/2015/daily_2015_3.Rdata")

ML_2015_3 <- 
  daily_2015_3 %>% 
  strr_expand(cores = 5) %>% 
  count(host_ID, date, listing_type) %>% 
  rename(count = n) 

rm(daily_2015_3)
save(ML_2015_3, file = "output/ML/ML_2015_3.Rdata")
rm(ML_2015_3)

load("output/ML/ML_2015_1.Rdata")
load("output/ML/ML_2015_2.Rdata")
load("output/ML/ML_2015_3.Rdata")

ML_2015 <- 
  rbind(ML_2015_1, ML_2015_2, ML_2015_3)

rm(ML_2015_1, ML_2015_2, ML_2015_3)


outliers_SR <- 
  ML_2015 %>% 
  filter(listing_type == "Shared room") %>% 
  count(host_ID, date) %>% 
  filter(n > 1)

outliers_PR <- 
  ML_2015 %>% 
  filter(listing_type == "Private room") %>% 
  count(host_ID, date) %>% 
  filter(n > 1)

outliers_EH <- 
  ML_2015 %>% 
  filter(listing_type == "Entire home/apt") %>% 
  count(host_ID, date) %>% 
  filter(n > 1)






ML_2015 %>% 
  filter(host_ID == "10003591", date == "2015-01-01")

library(upgo)

upgo_connect()

test_prop <- 
  property_all %>% 
  filter(host_ID == "10003591", listing_type == "Shared room") %>% 
  collect()

daily_all %>% 
  filter(property_ID %in% !! test_prop$property_ID, start_date == "2015-01-01",
         listing_type == "Shared room") %>% 
  collect()


daily_all %>% 
  filter(property_ID == "ab-1980267") %>% 
  collect() %>% 
  view()


daily_2015_1 %>% 
  filter(host_ID == "10003591", start_date == "2015-01-01", listing_type == "Shared room")

daily_2015_2 %>% 
  filter(host_ID == "10003591", start_date == "2015-01-01", listing_type == "Shared room")

daily_2015_3 %>% 
  filter(host_ID == "10003591", start_date == "2015-01-01", listing_type == "Shared room")


property %>% 
  filter(host_ID == "10003591") %>% 
  pull(listing_title)

ML_2015_compressed <- 
  ML_2015 %>% 
  strr_compress(cores = 6)

upgo_connect()

Sys.time()
ML_2015_compressed %>% 
  RPostgres::dbWriteTable(con, "ML", ., append = TRUE)
Sys.time()




