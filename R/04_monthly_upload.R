################################################################################
############################### MONTHLY UPLOADER ###############################
################################################################################



################################################################################
#### 1. LOAD LIBRARIES AND FILES AND PREPARE DATES #############################

### Load libraries #############################################################

library(tidyverse)
library(strr)
library(upgo)
library(RPostgres)
library(future)

plan(multiprocess)


### Declare date variables #####################################################

# Detect year_month from current date
year_month <- 
  paste0(substr(Sys.Date(), 1, 4), "_", substr(Sys.Date(), 6, 7)) %>% 
  {case_when(
    substr(., 6, 7) == "01" ~ paste0(substr(., 1, 3),
                                     as.numeric(substr(., 4, 4)) - 1, 
                                     "_12"),
    substr(., 6, 7) == "10" ~ paste0(substr(., 1, 5), "09"),
    substr(., 6, 6) == "1"  ~ paste0(substr(., 1, 5), 
                                     as.numeric(substr(., 6, 7)) - 1),
    TRUE ~ paste0(substr(., 1, 6), as.numeric(substr(., 7, 7)) - 1)
  )}

# Manually specify year_month if needed
# year_month <- "2020_03"

# Derive last_month using same pattern matching
last_month <- 
  year_month %>% 
  {case_when(
    substr(., 6, 7) == "01" ~ paste0(substr(., 1, 3),
                                     as.numeric(substr(., 4, 4)) - 1, 
                                     "_12"),
    substr(., 6, 7) == "10" ~ paste0(substr(., 1, 5), "09"),
    substr(., 6, 6) == "1"  ~ paste0(substr(., 1, 5), 
                                     as.numeric(substr(., 6, 7)) - 1),
    TRUE ~ paste0(substr(., 1, 6), as.numeric(substr(., 7, 7)) - 1)
  )}


### Load property file #########################################################

load(paste0("output/property/property_", year_month, ".Rdata"))


################################################################################
################################################################################



################################################################################
#### 2. UPLOAD PROPERTY FILE ###################################################

### Prepare queries ############################################################

property_drop <- 'DROP TABLE public.property;'

property_create <- 'CREATE TABLE public.property
(
    "property_ID" text COLLATE pg_catalog."default" NOT NULL,
    "host_ID" text COLLATE pg_catalog."default",
    listing_title text COLLATE pg_catalog."default",
    property_type text COLLATE pg_catalog."default",
    listing_type text COLLATE pg_catalog."default",
    created date,
    scraped date,
    housing boolean,
    latitude real,
    longitude real,
    country text COLLATE pg_catalog."default",
    region text COLLATE pg_catalog."default",
    city text COLLATE pg_catalog."default",
    neighbourhood text COLLATE pg_catalog."default",
    metro_area text COLLATE pg_catalog."default",
    currency text COLLATE pg_catalog."default",
    bedrooms real,
    bathrooms real,
    max_guests real,
    response_rate real,
    superhost boolean,
    premier_partner boolean,
    cancellation text COLLATE pg_catalog."default",
    security_deposit real,
    cleaning_fee real,
    extra_people_fee real,
    check_in_time text COLLATE pg_catalog."default",
    check_out_time text COLLATE pg_catalog."default",
    minimum_stay real,
    num_reviews real,
    num_photos real,
    instant_book boolean,
    rating real,
    ab_property text COLLATE pg_catalog."default",
    ab_host text COLLATE pg_catalog."default",
    ha_property text COLLATE pg_catalog."default",
    ha_host text COLLATE pg_catalog."default",
    CONSTRAINT "property_pkey" PRIMARY KEY ("property_ID")
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;'

property_perm_dw <- 'GRANT ALL ON TABLE public.property TO dwachsmuth;'
property_perm_ro <-  'GRANT SELECT ON TABLE public.property TO read_only;'

property_index_host_ID <-
  'CREATE INDEX "property_index_host_ID"
  ON public.property USING btree
  ("host_ID" COLLATE pg_catalog."default")
  TABLESPACE pg_default;'

property_index_country <-
  'CREATE INDEX property_index_country
  ON public.property USING btree
  (country COLLATE pg_catalog."default")
  TABLESPACE pg_default;'

property_index_region <- 
  'CREATE INDEX property_index_region
  ON public.property USING btree
  (region COLLATE pg_catalog."default")
  TABLESPACE pg_default;'

property_index_city <-
  'CREATE INDEX property_index_city
  ON public.property USING btree
  (city COLLATE pg_catalog."default")
  TABLESPACE pg_default;'


### Execute queries to remove old table and create new one #####################

upgo_connect(property = FALSE, daily = FALSE, host = FALSE, reviews = FALSE,
             remote = TRUE)

dbExecute(.con, property_drop)
dbExecute(.con, property_create)
dbExecute(.con, property_perm_dw)
dbExecute(.con, property_index_host_ID)
dbExecute(.con, property_index_country)
dbExecute(.con, property_index_region)
dbExecute(.con, property_index_city)

dbExecute(con, property_drop)
dbExecute(con, property_create)
dbExecute(con, property_perm_dw)
dbExecute(con, property_perm_ro)
dbExecute(con, property_index_host_ID)
dbExecute(con, property_index_country)
dbExecute(con, property_index_region)
dbExecute(con, property_index_city)

rm(property_drop, property_create, property_perm_dw, property_perm_ro, 
   property_index_host_ID, property_index_country, property_index_region,
   property_index_city)


### Upload table ###############################################################

Sys.time()
RPostgres::dbWriteTable(.con, "property", property, append = TRUE)
Sys.time()
RPostgres::dbWriteTable(con, "property", property, append = TRUE)
Sys.time()


################################################################################
################################################################################



################################################################################
#### 3. PREPARE DAILY FILE #####################################################

### Import daily file ##########################################################

daily <- 
  read_csv(paste0("data/daily_", year_month, ".gz"),
           col_types = "cDcDddcddc") %>% 
  select(`Property ID`, Date, Status, `Booked Date`, `Price (USD)`,
         `Reservation ID`)


### Produce daily and host files ###############################################

output <- 
  daily %>% 
  strr_process_daily(property)

daily <- 
  output[[1]] %>% 
  strr_compress()

daily_inactive <- 
  output[[2]] %>% 
  strr_compress()

host <- 
  output[[1]] %>% 
  strr_host() %>% 
  strr_compress()

host_inactive <- 
  output[[2]] %>% 
  strr_host() %>% 
  strr_compress()

save(daily, file = paste0("output/", substr(year_month, 1, 4), 
                          "/daily_", year_month, ".Rdata"))
save(daily_inactive, file = paste0("output/", substr(year_month, 1, 4), 
                          "/daily_inactive_", year_month, ".Rdata"))
save(host, file = paste0("output/", substr(year_month, 1, 4), 
                          "/host_", year_month, ".Rdata"))
save(host_inactive, file = paste0("output/", substr(year_month, 1, 4), 
                         "/host_inactive_", year_month, ".Rdata"))

write_csv(output[[3]], paste0("output/", substr(year_month, 1, 4), "/error_", 
                                year_month, ".csv")) 
write_csv(output[[4]], paste0("output/", substr(year_month, 1, 4), 
                                "/missing_rows_", year_month, ".csv"))

rm(output)


################################################################################
################################################################################



################################################################################
#### 4. UPLOAD DAILY FILE ######################################################

### Upload daily ###############################################################

upgo_connect(property = FALSE, daily = FALSE, host = FALSE, reviews = FALSE,
             remote = TRUE)

Sys.time()
daily %>% 
  RPostgres::dbWriteTable(.con, "daily", ., append = TRUE)
Sys.time()
daily %>% 
  RPostgres::dbWriteTable(con, "daily", ., append = TRUE)
Sys.time()

daily_local <- dplyr::tbl(.con, "daily")
daily_remote <- dplyr::tbl(con, "daily")

daily_local %>% 
  filter(property_ID == !! daily[1,]$property_ID,
         start_date == !! daily[1,]$start_date) %>% 
  collect() %>% 
  nrow()

daily_remote %>% 
  filter(property_ID == !! daily[1,]$property_ID,
         start_date == !! daily[1,]$start_date) %>% 
  collect() %>% 
  nrow()

rm(daily_local, daily_remote)


### Upload daily_inactive ######################################################

upgo_connect(property = FALSE, daily = FALSE, host = FALSE, reviews = FALSE,
             remote = TRUE)

Sys.time()
daily_inactive %>% 
  RPostgres::dbWriteTable(.con, "daily_inactive", ., append = TRUE)
Sys.time()
daily_inactive %>% 
  RPostgres::dbWriteTable(con, "daily_inactive", ., append = TRUE)
Sys.time()

daily_inactive_local <- dplyr::tbl(.con, "daily_inactive")
daily_inactive_remote <- dplyr::tbl(con, "daily_inactive")

daily_inactive_local %>% 
  filter(property_ID == !! daily_inactive[1,]$property_ID,
         start_date == !! daily_inactive[1,]$start_date) %>% 
  collect() %>% 
  nrow()

daily_inactive_remote %>% 
  filter(property_ID == !! daily_inactive[1,]$property_ID,
         start_date == !! daily_inactive[1,]$start_date) %>% 
  collect() %>% 
  nrow()

rm(daily_inactive_local, daily_inactive_remote)


### Upload host ################################################################

upgo_connect(property = FALSE, daily = FALSE, host = FALSE, reviews = FALSE,
             remote = TRUE)

Sys.time()
host %>% 
  filter(host_ID %in% property$host_ID) %>% 
  RPostgres::dbWriteTable(.con, "host", ., append = TRUE)
Sys.time()
host %>% 
  filter(host_ID %in% property$host_ID) %>% 
  RPostgres::dbWriteTable(con, "host", ., append = TRUE)
Sys.time()

host_local <- dplyr::tbl(.con, "host")
host_remote <- dplyr::tbl(con, "host")

host_local %>% 
  filter(host_ID == !! host[1,]$host_ID,
         start_date == !! host[1,]$start_date) %>% 
  collect() %>% 
  nrow()

host_remote %>% 
  filter(host_ID == !! host[1,]$host_ID,
         start_date == !! host[1,]$start_date) %>% 
  collect() %>% 
  nrow()

rm(host_local, host_remote)


### Upload host_inactive #######################################################

upgo_connect(property = FALSE, daily = FALSE, host = FALSE, reviews = FALSE,
             remote = TRUE)

Sys.time()
host_inactive %>% 
  filter(host_ID %in% property$host_ID) %>% 
  RPostgres::dbWriteTable(con, "host_inactive", ., append = TRUE)
Sys.time()

host_inactive_remote <- dplyr::tbl(con, "host_inactive")

host_inactive_remote %>% 
  filter(host_ID == !! host_inactive[1,]$host_ID,
         start_date == !! host_inactive[1,]$start_date) %>% 
  collect() %>% 
  nrow()

rm(host_inactive_remote)

upgo_disconnect()


################################################################################
################################################################################
################################################################################


