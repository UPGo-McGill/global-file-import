##### Final daily and host upload ##############################################

library(tidyverse)
library(data.table)
library(upgo)
library(strr)
library(RPostgres)
library(future)

plan(multisession)


### Initialize new daily table #################################################

daily_drop <- 'DROP TABLE public.daily;'

daily_build <- 
  'CREATE TABLE public.daily
(
    "property_ID" text COLLATE pg_catalog."default" NOT NULL,
    start_date date NOT NULL,
    end_date date,
    status text COLLATE pg_catalog."default",
    booked_date date,
    price integer,
    "res_ID" integer,
    "host_ID" text COLLATE pg_catalog."default",
    listing_type text COLLATE pg_catalog."default",
    housing boolean,
    country text COLLATE pg_catalog."default",
    region text COLLATE pg_catalog."default",
    city text COLLATE pg_catalog."default",
    CONSTRAINT daily_pkey PRIMARY KEY ("property_ID", start_date)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;'

daily_index_country <-
  'CREATE INDEX daily_ind_country
    ON public.daily USING btree
    (country COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;'

daily_index_region <-
  'CREATE INDEX daily_ind_region
    ON public.daily USING btree
    (region COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;'

daily_index_city <-
  'CREATE INDEX daily_ind_city
    ON public.daily USING btree
    (city COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;'

daily_perm_dw <- 'GRANT ALL ON TABLE public.daily TO dwachsmuth;'
daily_perm_ro <-  'GRANT SELECT ON TABLE public.daily TO read_only;'

upgo_connect(property = FALSE, daily = FALSE, host = FALSE)

dbExecute(con, daily_drop)
dbExecute(con, daily_build)
dbExecute(con, daily_index_country)
dbExecute(con, daily_index_region)
dbExecute(con, daily_index_city)
dbExecute(con, daily_perm_dw)
dbExecute(con, daily_perm_ro)

rm(daily_drop, daily_build, daily_index_country, daily_index_region, 
   daily_index_city, daily_perm_dw, daily_perm_ro)


### Initialize new daily_inactive table ########################################

daily_inactive_drop <- 'DROP TABLE public.daily_inactive;'

daily_inactive_build <- 
  'CREATE TABLE public.daily_inactive
(
    "property_ID" text COLLATE pg_catalog."default" NOT NULL,
    start_date date NOT NULL,
    end_date date,
    status text COLLATE pg_catalog."default",
    booked_date date,
    price integer,
    "res_ID" integer,
    "host_ID" text COLLATE pg_catalog."default",
    listing_type text COLLATE pg_catalog."default",
    housing boolean,
    country text COLLATE pg_catalog."default",
    region text COLLATE pg_catalog."default",
    city text COLLATE pg_catalog."default",
    CONSTRAINT daily_inactive_pkey PRIMARY KEY ("property_ID", start_date)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;'

daily_inactive_index_country <-
  'CREATE INDEX daily_inactive_ind_country
    ON public.daily_inactive USING btree
    (country COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;'

daily_inactive_index_region <-
  'CREATE INDEX daily_inactive_ind_region
    ON public.daily_inactive USING btree
    (region COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;'

daily_inactive_index_city <-
  'CREATE INDEX daily_inactive_ind_city
    ON public.daily_inactive USING btree
    (city COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;'

daily_inactive_perm_dw <- 
  'GRANT ALL ON TABLE public.daily_inactive TO dwachsmuth;'

daily_inactive_perm_ro <-  
  'GRANT SELECT ON TABLE public.daily_inactive TO read_only;'

upgo_connect(property = FALSE, daily = FALSE, host = FALSE)

dbExecute(con, daily_inactive_drop)
dbExecute(con, daily_inactive_build)
dbExecute(con, daily_inactive_index_country)
dbExecute(con, daily_inactive_index_region)
dbExecute(con, daily_inactive_index_city)
dbExecute(con, daily_inactive_perm_dw)
dbExecute(con, daily_inactive_perm_ro)

rm(daily_inactive_drop, daily_inactive_build, daily_inactive_index_country, 
   daily_inactive_index_region, daily_inactive_index_city,
   daily_inactive_perm_dw, daily_inactive_perm_ro)


### Initialize host table ######################################################

host_drop <- 'DROP TABLE public.host;'

host_build <- 
  'CREATE TABLE public.host
(
    "host_ID" text COLLATE pg_catalog."default",
    start_date date,
    end_date date,
    listing_type text COLLATE pg_catalog."default",
    housing boolean,
    count integer,
    CONSTRAINT host_pkey PRIMARY KEY ("host_ID", start_date, listing_type, housing)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;'

host_index_host_ID <-
  'CREATE INDEX "host_ind_host_ID"
  ON public.host USING btree
  ("host_ID" COLLATE pg_catalog."default")
  TABLESPACE pg_default;'

host_perm_dw <- 'GRANT ALL ON TABLE public.host TO dwachsmuth;'
host_perm_ro <-  'GRANT SELECT ON TABLE public.host TO read_only;'

upgo_connect(property = FALSE, daily = FALSE, host = FALSE)

dbExecute(con, host_drop)
dbExecute(con, host_build)
dbExecute(con, host_index_host_ID)
dbExecute(con, host_perm_dw)
dbExecute(con, host_perm_ro)

rm(host_drop, host_build, host_index_host_ID, host_perm_dw, host_perm_ro)


### Initialize host_inactive table #############################################

host_inactive_drop <- 'DROP TABLE public.host_inactive;'

host_inactive_build <- 
  'CREATE TABLE public.host_inactive
(
    "host_ID" text COLLATE pg_catalog."default",
    start_date date,
    end_date date,
    listing_type text COLLATE pg_catalog."default",
    housing boolean,
    count integer,
    CONSTRAINT host_inactive_pkey PRIMARY KEY ("host_ID", start_date, listing_type, housing)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;'

host_inactive_index_host_ID <-
  'CREATE INDEX "host_inactive_ind_host_ID"
  ON public.host USING btree
  ("host_ID" COLLATE pg_catalog."default")
  TABLESPACE pg_default;'

host_inactive_perm_dw <- 'GRANT ALL ON TABLE public.host_inactive TO dwachsmuth;'
host_inactive_perm_ro <-  'GRANT SELECT ON TABLE public.host_inactive TO read_only;'

upgo_connect(property = FALSE, daily = FALSE, host = FALSE)

dbExecute(con, host_inactive_drop)
dbExecute(con, host_inactive_build)
dbExecute(con, host_inactive_index_host_ID)
dbExecute(con, host_inactive_perm_dw)
dbExecute(con, host_inactive_perm_ro)

rm(host_inactive_drop, host_inactive_build, host_inactive_index_host_ID, 
   host_inactive_perm_dw, host_inactive_perm_ro, con)



################################################################################
#### PROCESSING LOOP ###########################################################

load("output/property/property_2019_12.Rdata")


### Set year ###################################################################

# For all years except 2017-2018 a single import will work

year <- "2019_12"


### Process daily ##############################################################

daily <- 
  read_csv(paste0("data/daily_", year, ".gz"), 
           col_types = "cDcDddcddc") %>% 
  select(`Property ID`, Date, Status, `Booked Date`, `Price (USD)`,
         `Reservation ID`)


## Deal with HA listings missing created date

if ((property %>% 
     filter(is.na(created) | is.na(scraped)) %>% 
     filter(property_ID %in% daily$`Property ID`) %>% 
     nrow()) > 0) {
  
  HA_created_NAs <- 
    property %>% 
    filter(is.na(created) | is.na(scraped)) %>% 
    filter(property_ID %in% daily$`Property ID`) %>% 
    pull(property_ID)
  
  property <- 
    daily %>% 
    filter(`Property ID` %in% HA_created_NAs, Status != "U") %>% 
    group_by(`Property ID`) %>% 
    filter(Date == min(Date)) %>% 
    select(property_ID = `Property ID`, date = Date) %>% 
    left_join(property, .) %>% 
    mutate(created = if_else(!is.na(date), date, created)) %>% 
    select(-date)
  
  save(property, file = "output/property/property_2019_12.Rdata")
  
  rm(HA_created_NAs)
  
}

output <- 
  daily %>% 
  strr_process_daily(property)

daily <- 
  output[[1]] %>% 
  strr_compress()

save(daily, 
     file = paste0("output/", substr(year, 1, 4), "/daily_", year, ".Rdata"))

daily_inactive <- 
  output[[2]] %>% 
  strr_compress()

save(daily_inactive, 
     file = paste0("output/", substr(year, 1, 4), "/daily_inactive_", year, 
                   ".Rdata"))

write_csv(output[[3]], paste0("output/", substr(year, 1, 4), "/error_", year, 
                              ".csv"))
write_csv(output[[4]], paste0("output/", substr(year, 1, 4), "/missing_rows_",
                              year, ".csv"))


### Process host ###############################################################

host <- 
  output[[1]] %>% 
  strr_host_2()

host <- 
  host %>% 
  strr_compress()

save(host, 
     file = paste0("output/", substr(year, 1, 4), "/host_", year, ".Rdata"))

host_inactive <- 
  output[[2]] %>% 
  strr_host_2()

host_inactive <- 
  host_inactive %>% 
  strr_compress()

save(host_inactive, 
     file = paste0("output/", substr(year, 1, 4), "/host_inactive_", year, 
                   ".Rdata"))


### Upload daily ###############################################################

upgo_connect(property = FALSE)

Sys.time()
daily %>% 
  RPostgres::dbWriteTable(con, "daily", ., append = TRUE)
Sys.time()

daily_all <- tbl(con, "daily")

daily_all %>% 
  filter(property_ID == !! daily[1,]$property_ID,
         start_date == !! daily[1,]$start_date) %>% 
  collect() %>% 
  nrow()


### Upload daily_inactive ######################################################

Sys.time()
daily_inactive %>% 
  RPostgres::dbWriteTable(con, "daily_inactive", ., append = TRUE)
Sys.time()

daily_inactive_all <- tbl(con, "daily_inactive")

daily_inactive_all %>% 
  filter(property_ID == !! daily_inactive[1,]$property_ID,
         start_date == !! daily_inactive[1,]$start_date) %>% 
  collect() %>% 
  nrow()


### Upload host ################################################################

Sys.time()
host %>% 
  RPostgres::dbWriteTable(con, "host", ., append = TRUE)
Sys.time()

host_all <- tbl(con, "host")

host_all %>% 
  filter(host_ID == !! host[1,]$host_ID,
         start_date == !! host[1,]$start_date) %>% 
  collect() %>% 
  nrow()


### Upload host_inactive #######################################################

Sys.time()
host_inactive %>% 
  RPostgres::dbWriteTable(con, "host_inactive", ., append = TRUE)
Sys.time()

host_inactive_all <- tbl(con, "host_inactive")

host_inactive_all %>% 
  filter(host_ID == !! host_inactive[1,]$host_ID,
         start_date == !! host_inactive[1,]$start_date) %>% 
  collect() %>% 
  nrow()


### Clean up ###################################################################

rm(con, daily, daily_inactive, host, host_inactive, output, daily_all,
   daily_inactive_all, host_all, host_inactive_all, year)







### Prepare file ###############################################################

## For 2017-2018, splitting of files is necessary
## Unzip .gz and split result into billion-row chunks

R.utils::gunzip(paste0("data/daily_", year, ".gz"), remove = FALSE)

# Not working yet
system2(
  "split -l 1000000000 Documents/Academic/Code/global-file-import/data/daily_2017"
  )


### Process daily_1 ############################################################

## Read chunk, strip its "problems" attr, and remove final PID

daily <- 
  read_csv("data/xaa", skip = 1,
           col_names = c("Property ID", "Date", "Status", "Booked Date",
                         "Price (USD)", "Price (Native)", "Currency Native",
                         "Reservation ID", "Airbnb Property ID",
                         "HomeAway Property ID"),
           col_types = "cDcDddcddc") %>% 
  select(`Property ID`, Date, Status, `Booked Date`, `Price (USD)`,
         `Reservation ID`)

attr(daily, "problems") <- 
  problems(daily) %>% 
  filter(expected != "10 columns")

last_PID <- 
  daily %>% 
  slice(nrow(daily)) %>% 
  pull("Property ID")

last_rows <- 
  daily %>% 
  filter(`Property ID` == last_PID)

attr(last_rows, "problems") <- NULL

save(last_rows, file = "temp_last_rows.Rdata")

daily <- 
  daily %>% 
  filter(`Property ID` != last_PID)


## Deal with HA listings missing created date

if ((property %>% 
     filter(is.na(created) | is.na(scraped)) %>% 
     filter(property_ID %in% daily$`Property ID`) %>% 
     nrow()) > 0) {
  
  HA_created_NAs <- 
    property %>% 
    filter(is.na(created) | is.na(scraped)) %>% 
    filter(property_ID %in% daily$`Property ID`) %>% 
    pull(property_ID)
  
  property <- 
    daily %>% 
    filter(`Property ID` %in% HA_created_NAs, Status != "U") %>% 
    group_by(`Property ID`) %>% 
    filter(Date == min(Date)) %>% 
    select(property_ID = `Property ID`, date = Date) %>% 
    left_join(property, .) %>% 
    mutate(created = if_else(!is.na(date), date, created)) %>% 
    select(-date)
  
  save(property, file = "output/property/property_2019_12.Rdata")
  
  rm(HA_created_NAs)
  
}


## Process daily file

output <- 
  daily %>% 
  strr_process_daily(property)

daily_1 <- 
  output[[1]] %>% 
  strr_compress()

save(daily_1, file = "daily_1_temp.Rdata")

daily_inactive_1 <- 
  output[[2]] %>% 
  strr_compress()

save(daily_inactive_1, file = "daily_inactive_1_temp.Rdata")

error_1 <- output[[3]]
missing_1 <- output[[4]]

save(error_1, missing_1, file = "error_1_temp.Rdata")


## Process host file

host_1 <- 
  output[[1]] %>% 
  strr_host()

host_1 <- 
  host_1 %>% 
  strr_compress()

host_inactive_1 <- 
  output[[2]] %>% 
  strr_host()

host_inactive_1 <- 
  host_inactive_1 %>% 
  strr_compress()

save(host_1, host_inactive_1, file = "host_1_temp.Rdata")

rm(daily, daily_1, daily_inactive_1, error_1, host_1, host_inactive_1, 
   last_rows, missing_1, output, last_PID)


### Process daily_2 ############################################################

## Read chunk, strip its "problems" attr, and remove final PID

daily <- 
  read_csv("data/xab", skip = 0,
           col_names = c("Property ID", "Date", "Status", "Booked Date",
                         "Price (USD)", "Price (Native)", "Currency Native",
                         "Reservation ID", "Airbnb Property ID",
                         "HomeAway Property ID"),
           col_types = "cDcDddcddc") %>% 
  select(`Property ID`, Date, Status, `Booked Date`, `Price (USD)`,
         `Reservation ID`)

attr(daily, "problems") <- 
  problems(daily) %>% 
  filter(expected != "10 columns")

last_PID <- 
  daily %>% 
  slice(nrow(daily)) %>% 
  pull("Property ID")

load("temp_last_rows.Rdata")

daily <- 
  bind_rows(last_rows, daily)

last_rows <- 
  daily %>% 
  filter(`Property ID` == last_PID)

attr(last_rows, "problems") <- NULL

save(last_rows, file = "temp_last_rows.Rdata")

daily <- 
  daily %>% 
  filter(`Property ID` != last_PID)


## Deal with HA listings missing created date

if ((property %>% 
     filter(is.na(created) | is.na(scraped)) %>% 
     filter(property_ID %in% daily$`Property ID`) %>% 
     nrow()) > 0) {
  
  HA_created_NAs <- 
    property %>% 
    filter(is.na(created) | is.na(scraped)) %>% 
    filter(property_ID %in% daily$`Property ID`) %>% 
    pull(property_ID)
  
  property <- 
    daily %>% 
    filter(`Property ID` %in% HA_created_NAs, Status != "U") %>% 
    group_by(`Property ID`) %>% 
    filter(Date == min(Date)) %>% 
    select(property_ID = `Property ID`, date = Date) %>% 
    left_join(property, .) %>% 
    mutate(created = if_else(!is.na(date), date, created)) %>% 
    select(-date)
  
  save(property, file = "output/property/property_2019_12.Rdata")
  
  rm(HA_created_NAs)
  
}



## Process daily file

output <- 
  daily %>% 
  strr_process_daily(property)

daily_2 <- 
  output[[1]] %>% 
  strr_compress()

save(daily_2, file = "daily_2_temp.Rdata")

daily_inactive_2 <- 
  output[[2]] %>% 
  strr_compress()

save(daily_inactive_2, file = "daily_inactive_2_temp.Rdata")

error_2 <- output[[3]]
missing_2 <- output[[4]]

save(error_2, missing_2, file = "error_2_temp.Rdata")


## Process host file

host_2 <- 
  output[[1]] %>% 
  strr_host()

host_2 <- 
  host_2 %>% 
  strr_compress()

host_inactive_2 <- 
  output[[2]] %>% 
  strr_host()

host_inactive_2 <- 
  host_inactive_2 %>% 
  strr_compress()

save(host_2, host_inactive_2, file = "host_2_temp.Rdata")
rm(output)


### Process daily_3 ############################################################

## Read chunk, strip its "problems" attr, and remove final PID

daily <- 
  read_csv("data/xac", skip = 0,
           col_names = c("Property ID", "Date", "Status", "Booked Date",
                         "Price (USD)", "Price (Native)", "Currency Native",
                         "Reservation ID", "Airbnb Property ID",
                         "HomeAway Property ID"),
           col_types = "cDcDddcddc") %>% 
  select(`Property ID`, Date, Status, `Booked Date`, `Price (USD)`,
         `Reservation ID`)

attr(daily, "problems") <- 
  problems(daily) %>% 
  filter(expected != "10 columns")

last_PID <- 
  daily %>% 
  slice(nrow(daily)) %>% 
  pull("Property ID")

load("temp_last_rows.Rdata")

daily <- 
  bind_rows(last_rows, daily)

last_rows <- 
  daily %>% 
  filter(`Property ID` == last_PID)

attr(last_rows, "problems") <- NULL

save(last_rows, file = "temp_last_rows.Rdata")

daily <- 
  daily %>% 
  filter(`Property ID` != last_PID)


## Deal with HA listings missing created date

if ((property %>% 
    filter(is.na(created) | is.na(scraped)) %>% 
    filter(property_ID %in% daily$`Property ID`) %>% 
    nrow()) > 0) {

  HA_created_NAs <- 
    property %>% 
    filter(is.na(created) | is.na(scraped)) %>% 
    filter(property_ID %in% daily$`Property ID`) %>% 
    pull(property_ID)
  
  property <- 
    daily %>% 
    filter(`Property ID` %in% HA_created_NAs, Status != "U") %>% 
    group_by(`Property ID`) %>% 
    filter(Date == min(Date)) %>% 
    select(property_ID = `Property ID`, date = Date) %>% 
    left_join(property, .) %>% 
    mutate(created = if_else(!is.na(date), date, created)) %>% 
    select(-date)
  
  save(property, file = "output/property/property_2019_12.Rdata")
  
  rm(HA_created_NAs)
  
}


## Process daily file

output <- 
  daily %>% 
  strr_process_daily(property)

rm(daily)

daily_3 <- 
  output[[1]] %>% 
  strr_compress()

save(daily_3, file = "daily_3_temp.Rdata")

daily_inactive_3 <- 
  output[[2]] %>% 
  strr_compress()

save(daily_inactive_3, file = "daily_inactive_3_temp.Rdata")

error_3 <- output[[3]]
missing_3 <- output[[4]]

save(error_3, missing_3, file = "error_3_temp.Rdata")


## Process host file

host_3 <- 
  output[[1]] %>% 
  strr_host()

host_3 <- 
  host_3 %>% 
  strr_compress()

host_inactive_3 <- 
  output[[2]] %>% 
  strr_host()

host_inactive_3 <- 
  host_inactive_3 %>% 
  strr_compress()

save(host_3, host_inactive_3, file = "host_3_temp.Rdata")

rm(daily_3, daily_inactive_3, error_3, host_3, host_inactive_3, last_rows,
   missing_3, output, last_PID)


### Process daily_4 ############################################################

## Read chunk, strip its "problems" attr, and remove final PID

daily <- 
  read_csv("data/xad", skip = 0,
           col_names = c("Property ID", "Date", "Status", "Booked Date",
                         "Price (USD)", "Price (Native)", "Currency Native",
                         "Reservation ID", "Airbnb Property ID",
                         "HomeAway Property ID"),
           col_types = "cDcDddcddc") %>% 
  select(`Property ID`, Date, Status, `Booked Date`, `Price (USD)`,
         `Reservation ID`)

attr(daily, "problems") <- 
  problems(daily) %>% 
  filter(expected != "10 columns")

load("temp_last_rows.Rdata")

daily <- 
  bind_rows(last_rows, daily)


## Deal with HA listings missing created date

if ((property %>% 
     filter(is.na(created) | is.na(scraped)) %>% 
     filter(property_ID %in% daily$`Property ID`) %>% 
     nrow()) > 0) {
  
  HA_created_NAs <- 
    property %>% 
    filter(is.na(created) | is.na(scraped)) %>% 
    filter(property_ID %in% daily$`Property ID`) %>% 
    pull(property_ID)
  
  property <- 
    daily %>% 
    filter(`Property ID` %in% HA_created_NAs, Status != "U") %>% 
    group_by(`Property ID`) %>% 
    filter(Date == min(Date)) %>% 
    select(property_ID = `Property ID`, date = Date) %>% 
    left_join(property, .) %>% 
    mutate(created = if_else(!is.na(date), date, created)) %>% 
    select(-date)
  
  save(property, file = "output/property/property_2019_12.Rdata")
  
  rm(HA_created_NAs)
  
}


## Process daily file

output <- 
  daily %>% 
  strr_process_daily(property)

rm(daily)

daily_4 <- 
  output[[1]] %>% 
  strr_compress()

save(daily_4, file = "daily_4_temp.Rdata")

daily_inactive_4 <- 
  output[[2]] %>% 
  strr_compress()

save(daily_inactive_4, file = "daily_inactive_4_temp.Rdata")

error_4 <- output[[3]]
missing_4 <- output[[4]]

save(error_4, missing_4, file = "error_4_temp.Rdata")


## Process host file

host_4 <- 
  output[[1]] %>% 
  strr_host()

host_4 <- 
  host_4 %>% 
  strr_compress()

host_inactive_4 <- 
  output[[2]] %>% 
  strr_host()

host_inactive_4 <- 
  host_inactive_4 %>% 
  strr_compress()

save(host_4, host_inactive_4, file = "host_4_temp.Rdata")




### Combine outputs ############################################################

load("daily_1_temp.Rdata")
load("daily_2_temp.Rdata")
load("daily_3_temp.Rdata")

daily <-
  bind_rows(daily_1, daily_2, daily_3, daily_4)

rm(daily_1, daily_2, daily_3, daily_4)

load("daily_inactive_1_temp.Rdata")
load("daily_inactive_2_temp.Rdata")
load("daily_inactive_3_temp.Rdata")
load("daily_inactive_4_temp.Rdata")

daily_inactive <-
  bind_rows(daily_inactive_1, daily_inactive_2, daily_inactive_3,
            daily_inactive_4)

rm(daily_inactive_1, daily_inactive_2, daily_inactive_3, daily_inactive_4)

load("error_1_temp.Rdata")
load("error_2_temp.Rdata")
load("error_3_temp.Rdata")
load("error_4_temp.Rdata")

error <-
  bind_rows(error_1, error_2, error_3, error_4)

missing_rows <-
  bind_rows(missing_1, missing_2, missing_3, missing_4)

rm(error_1, error_2, error_3, error_4, missing_1, missing_2, missing_3,
   missing_4)

load("host_1_temp.Rdata")
load("host_2_temp.Rdata")
load("host_3_temp.Rdata")
load("host_4_temp.Rdata")

host <-
  bind_rows(host_1, host_2, host_3, host_4)

rm(host_1, host_2, host_3, host_4)

host_redo <- 
  host %>% 
  group_by(host_ID, start_date, listing_type, housing) %>% 
  filter(n() > 1) %>% 
  ungroup()

host_redo <-
  host %>% 
  filter(host_ID %in% host_redo$host_ID)

host_redo_expanded <- 
  host_redo %>% 
  strr_expand()

host_redo_expanded <-
  host_redo_expanded %>% 
  group_by(host_ID, date, listing_type, housing) %>% 
  summarize(count = sum(count)) %>% 
  ungroup()

host_redo_compressed <- 
  host_redo_expanded %>% 
  strr_compress()

host <- 
  host %>% 
  filter(!host_ID %in% host_redo_compressed$host_ID) %>% 
  bind_rows(host_redo_compressed) %>% 
  arrange(host_ID, start_date)

rm(host_1, host_2, host_3, host_4, host_redo, host_redo_expanded, 
   host_redo_compressed)

host_inactive <-
  bind_rows(host_inactive_1, host_inactive_2, host_inactive_3, host_inactive_4)

host_inactive_redo <- 
  host_inactive %>% 
  group_by(host_ID, start_date, listing_type, housing) %>% 
  filter(n() > 1) %>% 
  ungroup()

host_inactive_redo <-
  host_inactive %>% 
  filter(host_ID %in% host_inactive_redo$host_ID)

host_inactive_redo_expanded <- 
  host_inactive_redo %>% 
  strr_expand()

host_inactive_redo_expanded <-
  host_inactive_redo_expanded %>% 
  group_by(host_ID, date, listing_type, housing) %>% 
  summarize(count = sum(count)) %>% 
  ungroup()

host_inactive_redo_compressed <- 
  host_inactive_redo_expanded %>% 
  strr_compress()

host_inactive <- 
  host_inactive %>% 
  filter(!host_ID %in% host_inactive_redo_compressed$host_ID) %>% 
  bind_rows(host_inactive_redo_compressed) %>% 
  arrange(host_ID, start_date)

rm(host_inactive_1, host_inactive_2, host_inactive_3, host_inactive_4,
   host_inactive_redo, host_inactive_redo_expanded, 
   host_inactive_redo_compressed)



### Save outputs ###############################################################

save(daily, 
     file = paste0("output/", substr(year, 1, 4), "/daily_", year, ".Rdata"))

save(daily_inactive, 
     file = paste0("output/", substr(year, 1, 4), "/daily_inactive_", year, 
                   ".Rdata"))

write_csv(error, paste0("output/", substr(year, 1, 4), "/error_", year, ".csv"))
write_csv(missing_rows, paste0("output/", substr(year, 1, 4), "/missing_rows_", 
                               year, ".csv"))

save(host, 
     file = paste0("output/", substr(year, 1, 4), "/host_", year, ".Rdata"))

save(host_inactive, 
     file = paste0("output/", substr(year, 1, 4), "/host_inactive_", year, 
                   ".Rdata"))



### Delete temp files and clean up #############################################



### Upload daily ###############################################################

upgo_connect(property = FALSE, daily = FALSE, host = FALSE)

Sys.time()
daily %>% 
  RPostgres::dbWriteTable(con, "daily_new", ., append = TRUE)
Sys.time()

daily_new_all <- tbl(con, "daily_new")

daily_new_all %>% 
  filter(property_ID == !! daily[1,]$property_ID,
         start_date == !! daily[1,]$start_date) %>% 
  collect() %>% 
  nrow()


### Upload daily_inactive ######################################################

Sys.time()
daily_inactive %>% 
  RPostgres::dbWriteTable(con, "daily_inactive", ., append = TRUE)
Sys.time()

daily_inactive_all <- tbl(con, "daily_inactive")

daily_inactive_all %>% 
  filter(property_ID == !! daily_inactive[1,]$property_ID,
         start_date == !! daily_inactive[1,]$start_date) %>% 
  collect() %>% 
  nrow()


### Upload host ################################################################

Sys.time()
host %>% 
  RPostgres::dbWriteTable(con, "host", ., append = TRUE)
Sys.time()

host_all <- tbl(con, "host")

host_all %>% 
  filter(host_ID == !! host[1,]$host_ID,
         start_date == !! host[1,]$start_date) %>% 
  collect() %>% 
  nrow()


### Upload host_inactive #######################################################

Sys.time()
host_inactive %>% 
  RPostgres::dbWriteTable(con, "host_inactive", ., append = TRUE)
Sys.time()

host_inactive_all <- tbl(con, "host_inactive")

host_inactive_all %>% 
  filter(host_ID == !! host_inactive[1,]$host_ID,
         start_date == !! host_inactive[1,]$start_date) %>% 
  collect() %>% 
  nrow()


### Clean up ###################################################################

rm(con, daily, daily_inactive, host, host_inactive, output, daily_new_all,
   daily_inactive_all, host_all, host_inactive_all, year)


################################################################################
################################################################################

