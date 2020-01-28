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
# year_month <- "2019_11"

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
    ab_property real,
    ab_host real,
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

upgo_connect(property = FALSE, daily = FALSE, multi = FALSE, reviews = FALSE)

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
RPostgres::dbWriteTable(con, "property", property, append = TRUE)
Sys.time()


################################################################################
################################################################################



################################################################################
#### 3. PREPARE DAILY FILE 1 ###################################################

### Import daily file ##########################################################

load("data/daily_length.Rdata")

daily <- read_csv(paste0("data/daily_", year_month, ".gz"), 
                  col_names = c("Property ID", "Date", "Status", "Booked Date",
                                "Price (USD)", "Price (Native)",
                                "Currency Native", "Reservation ID", 
                                "Airbnb Property ID", "HomeAway Property ID"),
                  col_types = "cDcDddcddc", skip = 1, 
                  n_max = floor(daily_length / 3)) %>% 
  select(`Property ID`, Date, Status, `Booked Date`, `Price (USD)`,
         `Reservation ID`)


### Prepare ML table ###########################################################

# ML_1 <- 
#   daily %>% 
#   set_names("property_ID", "date", "status", "booked_date", "price", 
#             "res_ID") %>% 
#   left_join(select(property, property_ID, host_ID, listing_type)) %>% 
#   count(host_ID, date, listing_type)
# 
# save(ML_1, file = "temp_ML_1.Rdata")
# 
# rm(ML_1)


### Compress daily file, join to property file, and save output ################

output_1 <- strr_compress(daily, cores = 5)

daily <- 
  output_1[[1]] %>% 
  inner_join(select(property, property_ID, host_ID, listing_type:housing,
                   country:city),
            by = "property_ID")

save(daily, file = paste0("output/", substr(year_month, 1, 4), 
                          "/daily_", year_month, "_1.Rdata"))
write_csv(output_1[[2]], paste0("output/", substr(year_month, 1, 4), "/error_", 
                                year_month, "_1.csv")) 
write_csv(output_1[[3]], paste0("output/", substr(year_month, 1, 4), 
                                "/missing_rows_", year_month, "_1.csv"))


### Add to no_property_ID file #################################################

load(paste0("output/2019/no_PID_", last_month, ".Rdata"))

no_property_ID <- 
  daily %>% 
  filter(!(property_ID %in% property$property_ID)) %>% 
  rbind(no_property_ID)

save(no_property_ID, file = "temp_no_PID.Rdata")

rm(output_1)


################################################################################
################################################################################



################################################################################
#### 4. PREPARE DAILY FILE 2 ###################################################

### Import daily file ##########################################################

load("data/daily_length.Rdata")

daily <- read_csv(paste0("data/daily_", year_month, ".gz"), 
                  col_names = c("Property ID", "Date", "Status", "Booked Date",
                                "Price (USD)", "Price (Native)",
                                "Currency Native", "Reservation ID", 
                                "Airbnb Property ID", "HomeAway Property ID"),
                  col_types = "cDcDddcddc", 
                  skip = 1 + floor(daily_length / 3),
                  n_max = floor(daily_length / 3)) %>% 
  select(`Property ID`, Date, Status, `Booked Date`, `Price (USD)`,
         `Reservation ID`)


### Prepare ML table ###########################################################

# ML_2 <- 
#   daily %>% 
#   set_names("property_ID", "date", "status", "booked_date", "price", 
#             "res_ID") %>% 
#   left_join(select(property, property_ID, host_ID, listing_type)) %>% 
#   count(host_ID, date, listing_type)
# 
# save(ML_2, file = "temp_ML_2.Rdata")
# 
# rm(ML_2)


### Compress daily file, join to property file, and save output ################

output_2 <- strr_compress(daily, cores = 5)

daily <- 
  output_2[[1]] %>% 
  inner_join(select(property, property_ID, host_ID, listing_type:housing,
                   country:city),
            by = "property_ID")

save(daily, file = paste0("output/", substr(year_month, 1, 4), 
                          "/daily_", year_month, "_2.Rdata"))
write_csv(output_2[[2]], paste0("output/", substr(year_month, 1, 4), "/error_", 
                                year_month, "_2.csv")) 
write_csv(output_2[[3]], paste0("output/", substr(year_month, 1, 4), 
                                "/missing_rows_", year_month, "_2.csv"))


### Add to no_property_ID file #################################################

load("temp_no_PID.Rdata")

no_property_ID <- 
  daily %>% 
  filter(!(property_ID %in% property$property_ID)) %>% 
  rbind(no_property_ID)

save(no_property_ID, file = "temp_no_PID.Rdata")

rm(output_2)


################################################################################
################################################################################



################################################################################
#### 5. PREPARE DAILY FILE 3 ###################################################

### Import daily file ##########################################################

load("data/daily_length.Rdata")

daily <- read_csv(paste0("data/daily_", year_month, ".gz"), 
                  col_names = c("Property ID", "Date", "Status", "Booked Date",
                                "Price (USD)", "Price (Native)",
                                "Currency Native", "Reservation ID", 
                                "Airbnb Property ID", "HomeAway Property ID"),
                  col_types = "cDcDddcddc", 
                  skip = 1 + 2 * floor(daily_length / 3)) %>% 
  select(`Property ID`, Date, Status, `Booked Date`, `Price (USD)`,
         `Reservation ID`)


### Prepare ML table ###########################################################

# ML_3 <- 
#   daily %>% 
#   set_names("property_ID", "date", "status", "booked_date", "price", 
#             "res_ID") %>% 
#   left_join(select(property, property_ID, host_ID, listing_type)) %>% 
#   count(host_ID, date, listing_type)
# 
# save(ML_3, file = "temp_ML_3.Rdata")
# 
# rm(ML_3)


### Compress daily file, join to property file, and save output ################

output_3 <- strr_compress(daily, cores = 4)

daily <- 
  output_3[[1]] %>% 
  inner_join(select(property, property_ID, host_ID, listing_type:housing,
                   country:city),
            by = "property_ID")

save(daily, file = paste0("output/", substr(year_month, 1, 4), 
                          "/daily_", year_month, "_3.Rdata"))
write_csv(output_3[[2]], paste0("output/", substr(year_month, 1, 4), "/error_", 
                                year_month, "_3.csv")) 
write_csv(output_3[[3]], paste0("output/", substr(year_month, 1, 4), 
                                "/missing_rows_", year_month, "_3.csv"))


### Add to no_property_ID file and export file #################################

no_property_ID <- 
  daily %>% 
  filter(!(property_ID %in% property$property_ID)) %>% 
  rbind(no_property_ID)

save(no_property_ID, file = paste0("output/", substr(year_month, 1, 4), 
                                   "/no_PID_", year_month, ".Rdata"))

file.remove("temp_no_PID.Rdata", "data/daily_length.Rdata")

rm(output_3, no_property_ID, daily, daily_length)


################################################################################
################################################################################



################################################################################
#### 6. UPLOAD DAILY FILES #####################################################

### Upload file 1 ##############################################################

load(paste0("output/", substr(year_month, 1, 4), "/daily_", year_month,
            "_1.Rdata"))

upgo_connect(property = FALSE, multi = FALSE, reviews = FALSE)

Sys.time()
daily %>% 
  filter(property_ID %in% property$property_ID) %>% 
  RPostgres::dbWriteTable(con, "daily", ., append = TRUE)
Sys.time()

daily_all %>% 
  filter(property_ID == !! daily[1,]$property_ID,
         start_date == !! daily[1,]$start_date) %>% 
  collect() %>% 
  nrow()


### Upload file 2 ##############################################################

load(paste0("output/", substr(year_month, 1, 4), "/daily_", year_month,
            "_2.Rdata"))

upgo_connect(property = FALSE, multi = FALSE, reviews = FALSE)

Sys.time()
daily %>% 
  filter(property_ID %in% property$property_ID) %>% 
  RPostgres::dbWriteTable(con, "daily", ., append = TRUE)
Sys.time()

daily_all %>% 
  filter(property_ID == !! daily[1,]$property_ID,
         start_date == !! daily[1,]$start_date) %>% 
  collect() %>% 
  nrow()


### Upload file 3 ##############################################################

load(paste0("output/", substr(year_month, 1, 4), "/daily_", year_month,
            "_3.Rdata"))

upgo_connect(property = FALSE, multi = FALSE, reviews = FALSE)

Sys.time()
daily %>% 
  filter(property_ID %in% property$property_ID) %>% 
  RPostgres::dbWriteTable(con, "daily", ., append = TRUE)
Sys.time()

daily_all %>% 
  filter(property_ID == !! daily[1,]$property_ID,
         start_date == !! daily[1,]$start_date) %>% 
  collect() %>% 
  nrow()

upgo_disconnect()


################################################################################
################################################################################



################################################################################
#### 7. PREPARE AND UPLOAD ML TABLE ############################################

### Load files #################################################################

load("temp_ML_1.Rdata")
load("temp_ML_2.Rdata")
load("temp_ML_3.Rdata")

### Bind and compress the three separate tables ################################

ML <- 
  bind_rows(ungroup(ML_1), ungroup(ML_2), ungroup(ML_3)) %>% 
  group_by(host_ID, date, listing_type) %>% 
  summarize(count = sum(n)) %>% 
  ungroup()

rm(ML_1, ML_2, ML_3)

ML <- 
  ML %>% 
  ungroup() %>% 
  strr_compress(cores = 6)


### Save final ML table to disk ################################################

save(ML, file = paste0("output/ML/ML_", year_month, ".Rdata"))

file.remove("temp_ML_1.Rdata")
file.remove("temp_ML_2.Rdata")
file.remove("temp_ML_3.Rdata")


### Produce updated no_ML file #################################################

load(paste0("output/ML/no_ML_", last_month, ".Rdata"))

no_ML <- 
  ML %>% 
  filter(!(host_ID %in% property$host_ID)) %>% 
  rbind(no_ML)

save(no_ML, file = paste0("output/ML/no_ML_", year_month, ".Rdata"))

rm(no_ML)


### Upload ML table ############################################################

upgo_connect(property = FALSE, daily = FALSE, ML = FALSE)

Sys.time()
ML %>% 
  filter(host_ID %in% property$host_ID) %>% 
  RPostgres::dbWriteTable(con, "ML", ., append = TRUE)
Sys.time()


upgo_disconnect()






################################################################################
################################################################################
################################################################################


