##### Final daily and host upload ##############################################

library(tidyverse)
library(data.table)
library(upgo)
library(strr)
library(future)
library(RPostgres)

plan(multiprocess)


### Initialize new daily table #################################################

daily_drop <- 'DROP TABLE public.daily_new;'

daily_build <- 
  'CREATE TABLE public.daily_new
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
    ON public.daily_new USING btree
    (country COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;'

daily_index_region <-
  'CREATE INDEX daily_ind_region
    ON public.daily_new USING btree
    (region COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;'

daily_index_city <-
  'CREATE INDEX daily_ind_city
    ON public.daily_new USING btree
    (city COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;'

daily_perm_dw <- 'GRANT ALL ON TABLE public.daily_new TO dwachsmuth;'
daily_perm_ro <-  'GRANT SELECT ON TABLE public.daily_new TO read_only;'

upgo_connect(property = FALSE, daily = FALSE, multi = FALSE)

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

upgo_connect(property = FALSE, daily = FALSE, multi = FALSE)

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

upgo_connect(property = FALSE, daily = FALSE, multi = FALSE)

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

upgo_connect(property = FALSE, daily = FALSE, multi = FALSE)

dbExecute(con, host_inactive_drop)
dbExecute(con, host_inactive_build)
dbExecute(con, host_inactive_index_host_ID)
dbExecute(con, host_inactive_perm_dw)
dbExecute(con, host_inactive_perm_ro)

rm(host_inactive_drop, host_inactive_build, host_inactive_index_host_ID, 
   host_inactive_perm_dw, host_inactive_perm_ro, con)



################################################################################
#### PROCESSING LOOP ###########################################################

### Set year ###################################################################

year <- 2014

  
### Process daily ##############################################################

load("output/property/property_2019_11.Rdata")

daily <- 
  read_csv(paste0("data/daily_", year, ".gz"), 
           col_types = "cDcDddcddc") %>% 
  select(`Property ID`, Date, Status, `Booked Date`, `Price (USD)`,
         `Reservation ID`)

output <- 
  daily %>% 
  strr_process_daily(property)

daily <- 
  output[[1]] %>% 
  strr_compress()

save(daily, 
     file = paste0("output/", year, "/daily_", year, ".Rdata"))

daily_inactive <- 
  output[[2]] %>% 
  strr_compress()

save(daily_inactive, 
     file = paste0("output/", year, "/daily_inactive_", year, ".Rdata"))

write_csv(output[[3]], paste0("output/", year, "/error_", year, ".csv"))
write_csv(output[[4]], paste0("output/", year, "/missing_rows_", year, ".csv"))


### Process host ###############################################################

host <- 
  output[[1]] %>% 
  strr_host()

host <- 
  host %>% 
  strr_compress()

save(host, 
     file = paste0("output/", year, "/host_", year, ".Rdata"))

host_inactive <- 
  output[[2]] %>% 
  strr_host()

host_inactive <- 
  host_inactive %>% 
  strr_compress()

save(host_inactive, 
     file = paste0("output/", year, "/host_inactive_", year, ".Rdata"))


### Upload daily ###############################################################

upgo_connect(property = FALSE, daily = FALSE, multi = FALSE)

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

