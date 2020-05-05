################################################################################
########################### MONTHLY IMPORTER, PART 2 ###########################
################################################################################



################################################################################
#### 1. LOAD LIBRARIES AND FILES AND PREPARE DATES #############################

library(tidyverse)
library(sf)
library(upgo)
library(strr)
library(future)
library(furrr)

plan(multiprocess)
options(future.globals.maxSize = Inf)

load("temp_3.Rdata")

# Detect year_month from current date
year_month <- 
  paste0(substr(Sys.Date(), 1, 4), "_", substr(Sys.Date(), 6, 7)) %>% 
  {case_when(
    substr(., 6, 7) == "01" ~ paste0(substr(., 1, 2),
                                     as.numeric(substr(., 3, 4)) - 1, 
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
    substr(., 6, 7) == "01" ~ paste0(substr(., 1, 2),
                                     as.numeric(substr(., 3, 4)) - 1, 
                                     "_12"),
    substr(., 6, 7) == "10" ~ paste0(substr(., 1, 5), "09"),
    substr(., 6, 6) == "1"  ~ paste0(substr(., 1, 5), 
                                     as.numeric(substr(., 6, 7)) - 1),
    TRUE ~ paste0(substr(., 1, 6), as.numeric(substr(., 7, 7)) - 1)
  )}


################################################################################
################################################################################



################################################################################
#### 2. GEOCODE COUNTRY FIELD IN PROPERTY FILE #################################

### Load country boundaries ####################################################

load("data/country_boundaries.Rdata")

countries_active <- 
  countries %>% 
  filter(name %in% {
    property %>% 
      filter(!is.na(country)) %>% 
      pull(country) %>% 
      unique() %>% 
      sort()
    })


### Check listing intersection with country field entry ########################

## Create list of subdivided polygons for each country with listings

country_list <- 
  countries_active %>% 
  group_split(name) %>% 
  future_map(~{
    .x %>% 
      st_make_valid() %>% 
      lwgeom::st_subdivide(100) %>% 
      st_collection_extract("POLYGON") %>% 
      st_set_agr("identity")
  })


## Create paired list of listings and polygons for each country

pair_list <- 
  future_map2({property %>% filter(!is.na(country)) %>% group_split(country)}, 
       country_list, 
       list)

pair_list <- 
  pair_list[order(map_int(pair_list, ~{nrow(.x[[1]])}))]


## Calculate intersections of listings with specified country

country_matches_1 <-
  pair_list %>% 
  future_map_dfr(~{
    .x[[1]] %>% 
      strr_as_sf(3857) %>% 
      st_intersection(.x[[2]]) %>% 
      st_drop_geometry() %>% 
      select(property_ID, country, region, city)
      }, .progress = TRUE) %>% 
  distinct(property_ID, .keep_all = TRUE)

no_matches <- 
  property %>% 
  filter(!property_ID %in% country_matches_1$property_ID)


### Save temporary output ######################################################

temp_4_flag %<-% {
  save(country_matches_1, no_matches, file = "temp_4.Rdata")
  TRUE
}

rm(countries_active, country_list, pair_list)


################################################################################
################################################################################



################################################################################
#### 3. SECONDARY GEOGRAPHY TESTING ############################################

### Run a match against all countries for the listings with no matches #########

no_matches_list <- 
  suppressWarnings(
    no_matches %>%
      split(1:(nrow(.)/1000))
  )

country_matches_2 <- 
  no_matches_list %>% 
  future_map_dfr(~{
    .x %>% 
      strr_as_sf(3857) %>% 
      st_intersection(countries_sub) %>% 
      st_drop_geometry() %>% 
      select(property_ID, country, region, city, name)
    }, .progress = TRUE)

no_matches <- 
  no_matches %>% 
  filter(!property_ID %in% country_matches_2$property_ID)


### Save temporary output ######################################################

temp_5_flag %<-% {
  save(country_matches_2, no_matches, file = "temp_5.Rdata")
  TRUE
}


### Third try, against GADM 5 km borders #######################################

no_matches_list <- 
  suppressWarnings(
    no_matches %>%
      split(1:(nrow(.)/1000)) 
  )

country_matches_3 <- 
  no_matches_list %>% 
  future_map_dfr(~{
    .x %>% 
      strr_as_sf(3857) %>% 
      st_intersection(country_buffer_sub) %>% 
      st_drop_geometry() %>% 
      select(property_ID, country, region, city, name)
  }, .progress = TRUE)


## Decide ties based on closest distance

ties <- 
  country_matches_3 %>% 
  group_by(property_ID) %>% 
  filter(n() > 1)

ties_list <- 
  suppressWarnings(
    ties %>% 
      summarize(candidates = list(name)) %>% 
      split(1:100) %>% 
      future_map(function(x) {
        x %>% 
          mutate(winner = map2_chr(property_ID, candidates, ~{
            index <- 
              no_matches %>% 
              filter(property_ID == .x) %>% 
              strr_as_sf(3857) %>% 
              st_nearest_feature(countries %>% filter(name %in% unlist(.y)))
            (countries %>% filter(name %in% unlist(.y)))[index,]$name
            }))})
    )

country_matches_3 <- 
  ties %>% 
  ungroup() %>% 
  arrange(property_ID) %>% 
  group_by(property_ID, country, region, city) %>% 
  summarize() %>% 
  ungroup() %>% 
  left_join(bind_rows(ties_list), by = "property_ID") %>% 
  select(-candidates) %>% 
  rename(name = winner) %>% 
  rbind(country_matches_3 %>% group_by(property_ID) %>% filter(n() == 1) 
        %>% ungroup())

no_matches <- 
  no_matches %>% 
  filter(!property_ID %in% country_matches_3$property_ID)


### Save temporary output ######################################################

temp_6_flag %<-% {
  save(country_matches_3, no_matches, ties, file = "temp_6.Rdata")
  TRUE
}

rm(ties_list, no_matches_list)


### Fourth try, with rnaturalearth boundary files ##############################

no_matches_list <- 
  suppressWarnings(
    no_matches %>%
      split(1:(nrow(.)/100))
  )
  
country_matches_4 <- 
  no_matches_list %>% 
  future_map_dfr(~{
    .x %>% 
      strr_as_sf(3857) %>% 
      st_intersection(countries_2_sub) %>% 
      st_drop_geometry() %>% 
      select(property_ID, country, region, city, name)
  }, .progress = TRUE)

no_matches <- 
  no_matches %>% 
  filter(!property_ID %in% country_matches_4$property_ID)


### Save temporary output ######################################################

temp_7_flag %<-% {
  save(country_matches_4, no_matches, file = "temp_7.Rdata")
  TRUE
}


### Final try, with 5 km buffers around rnaturalearth boundary files ###########

no_matches_list <- 
  suppressWarnings(
    no_matches %>%
      split(1:(nrow(.)/100))  
  )
  
country_matches_5 <- 
  no_matches_list %>% 
  future_map_dfr(~{
    .x %>% 
      strr_as_sf(3857) %>% 
      st_intersection(country_2_buffer_sub) %>% 
      st_drop_geometry() %>% 
      select(property_ID, country, region, city, name)
  }, .progress = TRUE)


## Decide ties based on closest distance

ties_2 <- 
  country_matches_5 %>% 
  group_by(property_ID) %>% 
  filter(n() > 1) 

if (nrow(ties_2) != 0) {
  ties_list <- 
    suppressWarnings(
      ties_2 %>% 
        summarize(candidates = list(name)) %>% 
        split(1:100) %>% 
        furrr::future_map(function(x) {
          x %>% 
            mutate(winner = map2_chr(property_ID, candidates, ~{
              index <- 
                no_matches %>% 
                filter(property_ID == .x) %>% 
                strr_as_sf(3857) %>% 
                st_nearest_feature(countries_2 %>% filter(name %in% unlist(.y)))
              (countries_2 %>% filter(name %in% unlist(.y)))[index,]$name
            }))})
    )
    
  country_matches_5 <- 
    ties_2 %>% 
    ungroup() %>% 
    arrange(property_ID) %>% 
    group_by(property_ID, country, region, city) %>% 
    summarize() %>% 
    ungroup() %>% 
    left_join(bind_rows(ties_list)) %>% 
    select(-candidates) %>% 
    rename(name = winner) %>% 
    rbind(country_matches_5 %>% group_by(property_ID) %>% filter(n() == 1) 
          %>% ungroup())
  
}

ties <- 
  ties %>% 
  ungroup() %>% 
  rbind(ungroup(ties_2))

no_matches <- 
  no_matches %>% 
  filter(!property_ID %in% country_matches_5$property_ID)


### Save temporary output ######################################################

temp_8_flag %<-% {
  save(country_matches_5, ties, no_matches, file = "temp_8.Rdata")
  TRUE
}

suppressWarnings(
  rm(no_matches_list, ties_list, ties_2, countries, countries_2,
     countries_2_sub, countries_sub, country_2_buffer_list, country_buffer_list,
     ties)
  )


### Assemble geocoded locations ################################################

country_matches <- 
  bind_rows(
    mutate(country_matches_1, name = country),
    country_matches_2,
    country_matches_3,
    country_matches_4,
    country_matches_5,
    no_matches %>% 
      select(property_ID) %>% 
      mutate(country = NA_character_,
             region = NA_character_,
             city = NA_character_,
             name = NA_character_)
  ) %>% 
  rename(country_geo = name)


### Save temporary output ######################################################

temp_9_flag %<-% {
  save(country_matches, file = "temp_9.Rdata")
  TRUE
}

rm(country_matches_1, country_matches_2, country_matches_3, country_matches_4,
   country_matches_5, no_matches)


################################################################################
################################################################################



################################################################################
#### 4. RECONCILE SCRAPED AND GEOLOCATED COUNTRY NAMES #########################

### Load scrape results ########################################################

load("data/scrape_results.Rdata")


### Produce comparison table ###################################################

country_comparison <- 
  country_matches %>% 
  left_join(scrape_results %>% 
              filter(!is.na(raw)) %>% 
              select(property_ID, country_scrape = country),
            by = "property_ID") %>% 
  # Deal with weird islands
  mutate(
    country_scrape = if_else(country_scrape == "France" & country_geo %in% c(
      "French Guiana", "Guadeloupe", "Martinique", "Mayotte", "Réunion", 
      "Saint Barthélemy", "Saint Martin", "Suriname"), country_geo, 
      country_scrape),
    country_scrape = if_else(country_scrape == "United Kingdom" & 
                               country_geo == "Guernsey", country_geo, 
                             country_scrape),
    country_scrape = if_else(country_scrape == "United States" &
                               country_geo == "U.S. Virgin Islands",
                             country_geo, country_scrape),
    country_scrape = if_else(country_scrape == "China" & 
                               country_geo == "Taiwan" &
                               region == "Fujian", country_geo, country_scrape),
    country_scrape = if_else(
      country_scrape == "Bonaire Sint Eustatius and Saba",
      "Bonaire, Sint Eustatius and Saba", country_scrape)
    )


## Find listings with scrapes which disagree with geolocation

country_no_agree <- 
  country_comparison %>% 
  filter(!is.na(country_scrape), !is.na(country_geo), 
         country_geo != country_scrape)


### Test scraped countries against 5 km buffers ################################

## Intersect country_no_agree with GADM buffers

country_no_agree_valid <- 
  property %>% 
  filter(property_ID %in% country_no_agree$property_ID) %>% 
  strr_as_sf(3857) %>% 
  st_intersection(country_buffer_sub) %>% 
  st_drop_geometry() %>% 
  select(property_ID, country_scrape = name) %>% 
  inner_join(country_no_agree, ., by = c("property_ID", "country_scrape"))

# Otherwise use the geolocated country name
country_no_agree <- 
  country_no_agree %>% 
  filter(!property_ID %in% country_no_agree_valid$property_ID)


## Use rnaturalearth as second try

country_no_agree_valid_2 <- 
  property %>% 
  filter(property_ID %in% country_no_agree$property_ID) %>% 
  strr_as_sf(3857) %>% 
  st_intersection(country_2_buffer_sub) %>% 
  st_drop_geometry() %>% 
  select(property_ID, country_scrape = name) %>% 
  inner_join(country_no_agree, ., by = c("property_ID", "country_scrape")) 

country_no_agree <- 
  country_no_agree %>% 
  filter(!property_ID %in% country_no_agree_valid_2$property_ID)


### Reassemble results #########################################################

## For validated scrapes, use scraped location, otherwise use geolocation

country_fixed <-
  country_comparison %>% 
  filter(!property_ID %in% country_no_agree_valid$property_ID,
         !property_ID %in% country_no_agree_valid_2$property_ID) %>% 
  mutate(country = country_geo) %>% 
  mutate(region = if_else(is.na(country), NA_character_, region),
         city = if_else(is.na(country), NA_character_, city)) %>% 
  rbind(
    country_no_agree_valid %>% mutate(country = country_scrape),
    country_no_agree_valid_2 %>% mutate(country = country_scrape)) %>% 
  select(-country_geo, -country_scrape)


### Reassemble full location_table #############################################

location_table <- 
  country_fixed %>% 
  left_join(select(property, property_ID, latitude, longitude), 
            by = "property_ID") %>% 
  select(property_ID, latitude, longitude, country, region, city)


### Join location table back into property file ################################

property <-
  property %>% 
  select(-country, -region, -city) %>%
  # Inner join with length checking to catch mistakes
  inner_join(location_table, by = c("property_ID", "latitude", "longitude")) %>% 
  {
    if (nrow(.) == nrow(property)) {
      select(., property_ID:longitude, country:city, everything())
    } else {
      stop("Rows are missing from joined output.")
    }}


### Save temporary output ######################################################

temp_10_flag %<-% {
  save(property, file = "temp_10.Rdata")
  TRUE
}

rm(country_2_buffer_sub, country_buffer_sub, country_comparison, country_fixed,
   country_matches, country_no_agree, country_no_agree_valid, 
   country_no_agree_valid_2, location_table, temp_4_flag, temp_5_flag,
   temp_6_flag, temp_7_flag, temp_8_flag, temp_9_flag)


################################################################################
################################################################################



################################################################################
#### 5. FILL IN MISSING REGIONS IN CANADA ######################################

### Load geometry ##############################################################

province_names <- c("Alberta", "British Columbia", "Manitoba", "New Brunswick",
                    "Newfoundland and Labrador", "Northwest Territories", 
                    "Nova Scotia", "Nunavut", "Ontario", "Prince Edward Island",
                    "Québec", "Saskatchewan", "Yukon")

provinces <- 
  cancensus::get_census(
    "CA16", regions = list(C = "01"), level = "PR", geo_format = "sf",
    quiet = TRUE) %>% 
  mutate(name = if_else(name == "Quebec", "Québec", name)) %>% 
  select(name) %>% 
  st_set_agr("identity") %>% 
  st_transform(3857)


### Do geolocation and comparison ##############################################

## Get geolocation results for all Canada listings

Canada_provinces <- 
  property %>% 
  filter(country == "Canada") %>% 
  strr_as_sf(3857) %>% 
  st_nearest_feature(provinces) %>% 
  future_map_chr(~{provinces[.x,]$name})


## Create comparison table with AirDNA regions, geolocation and scraping

Canada_comparison <- 
  property %>% 
  filter(country == "Canada") %>% 
  select(property_ID, region:city) %>% 
  mutate(region_geo = Canada_provinces) %>% 
  left_join(select(scrape_results, property_ID, region_scrape = region, 
                   city_scrape = city), by = "property_ID") %>% 
  mutate(region_scrape = case_when(
    region_scrape %in% c("Ab", "AL", "alberta", "ALberta", "Alberta, Canada",
                         "An", "ALBERTA", "Calgary", "Edmonton",
                         "Grande Prairie") ~ 
      "Alberta",
    
    region_scrape %in% c("B C", "B.C", "B.C.", "bc", "Bc", "BC Canada",
                         "BC V0N2P0", "BC V1L", "BC,", "BC, North Saanich",
                         "BC/Canada", "British Colombia", "British Coloumbia", 
                         "British Columbia", "British Columbia ", 
                         "British Columbia (BC)", "British Columbia,",
                         "Burnaby", "Chilliwack",
                         "Colombie-Britannique", "Colombie Britannique", 
                         "Kamloops", "Kelowna",
                         " Invermere", "Invermere", "Squamish BC", "Vancouver",
                         "Surrey", "North Vancouver") ~ 
      "British Columbia",
    
    region_scrape %in% c("Manitona", "Mb", "Maniitoba", "manitoba", "Winnipeg",
                         "WInnipeg") ~
      "Manitoba",
    
    region_scrape %in% c("Nouveau-Brunswick", "NEW BRUNSWICK", "New brunswick",
                         "New-Brunswick") ~ 
      "New Brunswick",
    
    region_scrape %in% c("Newfoundland", "Newfound Land", "Nl", 
                         "Newfoundland ") ~
      "Newfoundland and Labrador",
    
    region_scrape %in% c("Cape Breton", "Cape Breton    NS", "Halifax", 
                         "Lunenburg, NS", "Nouvelle-Écosse", "Nova scotia", 
                         "Nova Scotia", "Ns", "NS- 316", "NS,") ~ 
      "Nova Scotia",
    
    region_scrape %in% c("Cornwall", "Dundas", "East Gwillimbury", "Eganville", 
                         "En Ontario.", "Greater Sudbury", "Oakville", "on", 
                         "On", "ON", "ON / Prince Edward County", " Ontario", 
                         "ON,", "ON, Canada", "ont", "Ont", "ontario", 
                         "Ontario", "ONtario", "ONTARIO", "Ontario,", 
                         "ONTARIO,", "ONTario", "ontario, canada", "Onterio", 
                         "Prince Edward County", "Prince Edward County ON", 
                         "Toronto", "TORONTO", "Ottawa", "Sudbury", 
                         "Thunder Bay") ~ 
      "Ontario",
    
    region_scrape %in% c("PEI") ~
      "Prince Edward Island",
    
    region_scrape %in% c("Cantons-de-l'est", "charlevoix Québec",  "Gatineau",
                         "Lachine", "LaSalle,  Quebec", "Longueuil", "Montreal", 
                         "Montreal, Québec", "Montréal", "P.Québec", "PQ",
                         "Quebec", " Québec ", "-Quebec", "qc", "Qc", "QC ", 
                         "QC,", "Qu", "QuÃ©bec", "Que", "quebec", "QUEBEC", 
                         "Quebéc", "québec", "Québec", "QuÈbec", "Québec ", 
                         "Québec City", "Quebec, Canada", "Quecbec",
                         "Ville de Québec") ~ 
      "Québec", 
    
    region_scrape %in% c("Sk", "Saskatchwan", "Saskatoon") ~
      "Saskatchewan",
    
    region_scrape %in% c("Yukon Territory", "Yukon Territories") ~ 
      "Yukon",
    
    TRUE ~ region_scrape
  ))


## Identify cases with agreement or with disagreement

Canada_agree <-
  Canada_comparison %>% 
  filter((region == region_geo & region == region_scrape) |
           (is.na(region_scrape) & region == region_geo))

Canada_no_agree <- 
  Canada_comparison %>% 
  filter(!property_ID %in% Canada_agree$property_ID)


## Intersect Canada_no_agree with 5 km buffer around provincial boundaries

Canada_no_agree_valid <- 
  property %>% 
  filter(property_ID %in% Canada_no_agree$property_ID) %>% 
  strr_as_sf(3857) %>% 
  st_intersection(st_buffer(provinces, 5000)) %>% 
  st_drop_geometry() %>% 
  select(property_ID, name) %>% 
  inner_join(select(Canada_no_agree, property_ID, name = region_scrape),
             by = c("property_ID", "name"))

Canada_no_agree <- 
  Canada_no_agree %>% 
  filter(!property_ID %in% Canada_no_agree_valid$property_ID)


## Accept geolocation result for remaining properties

Canada_finished <-
  Canada_agree %>% select(-region_geo, -region_scrape, -city_scrape) %>% 
  rbind(Canada_no_agree_valid %>% rename(region = name) %>% mutate(city = NA),
        Canada_no_agree %>% select(property_ID, region = region_geo, city))


### Bind results back into property table ######################################

property <-
  property %>% 
  filter(property_ID %in% Canada_finished$property_ID) %>% 
  select(-region, -city) %>% 
  inner_join(Canada_finished, by = "property_ID") %>% 
  select(property_ID:country, region, city, everything()) %>% 
  bind_rows(property %>% filter(!property_ID %in% Canada_finished$property_ID))


### Save temporary output ######################################################

temp_11_flag %<-% {
  save(property, file = "temp_11.Rdata")
  TRUE
}

rm(Canada_agree, Canada_comparison, Canada_finished, Canada_no_agree,
   Canada_no_agree_valid, provinces, Canada_provinces, province_names)


################################################################################
################################################################################



################################################################################
#### 6. FILL IN MISSING REGIONS IN USA #########################################

### Load geometry ##############################################################

state_names <- 
  c("Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", 
    "Connecticut", "Delaware", "District of Columbia", "Florida", "Georgia", 
    "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", 
    "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire",
    "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota",
    "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", 
    "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", 
    "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming")

states <- 
  tigris::states(class = "sf", progress_bar = FALSE) %>% 
  as_tibble() %>% 
  st_as_sf() %>% 
  st_transform(3857) %>% 
  select(NAME) %>% 
  st_set_agr("identity")


### Do geolocation and comparison ##############################################

## Get geolocation results for all US listings

US_states <- 
  suppressWarnings(
    property %>% 
      filter(country == "United States") %>% 
      split(1:(nrow(.) / 1000))  
  )
  
US_states <-
  US_states %>% 
  future_map(function(x) {
    x %>% 
      strr_as_sf(3857) %>% 
      st_intersects(states) %>% 
      map_chr(~{if (length(.x) == 0) NA_character_ else states[.x,]$NAME})
  }) %>% 
  map2_df(US_states, ~{.y %>% mutate(NAME = .x)}) %>% 
  filter(!is.na(NAME))

US_nearest <- 
  suppressWarnings(
    property %>% 
      filter(country == "United States", 
             !property_ID %in% US_states$property_ID) %>% 
      split(1:(nrow(.) / 10))  
  )
  
US_nearest <- 
  US_nearest %>% 
  future_map(function(x) {
    x %>% 
      strr_as_sf(3857) %>% 
      st_nearest_feature(states) %>% 
      map_chr(~{states[.x,]$NAME})
  }) %>% 
  map2_df(US_nearest, ~{.y %>% mutate(NAME = .x)})

US_states <- 
  bind_rows(US_states, US_nearest)


## Create comparison table with AirDNA regions, geolocation and scraping

US_comparison <- 
  property %>% 
  filter(country == "United States") %>% 
  select(property_ID, region:city) %>% 
  left_join(select(US_states, property_ID, region_geo = NAME),
            by = "property_ID") %>% 
  left_join(select(scrape_results, property_ID, region_scrape = region, 
                   city_scrape = city),
            by = "property_ID") %>% 
  mutate(region_scrape = case_when(
    region_scrape %in% c("Al") ~ "Alabama",
    region_scrape %in% c("Ak", "Anchorage") ~ "Alaska",
    region_scrape %in% c("Ar") ~ "Arkansas",
    region_scrape %in% c("AZ", "Az", "Phoenix") ~ "Arizona",
    region_scrape %in% c("Ca", "ca", "Anaheim", "Beverly Hills", 
                         "California ", "Los Angeles", "San Diego") ~ 
      "California",
    region_scrape %in% c("Co", "co", "CO", "CO ", "Boulder", "Denver",
                         "Fort Collins") ~ "Colorado",
    region_scrape %in% c("Fl", "fl", " Florida ", "Cape Canaveral", 
                         "Cape Coral", "Cocoa Beach", "Davenport", "Miami",
                         "Miami Beach", "Panama City Beach") ~ "Florida",
    region_scrape %in% c("Ga", "Atlanta", "Savannah") ~ "Georgia",
    region_scrape %in% c("Hi", "Hi ") ~ "Hawaii",
    region_scrape %in% c("Id") ~ "Idaho",
    region_scrape %in% c("Il", "Chicago") ~ "Illinois",
    region_scrape %in% c("In") ~ "Indiana",
    region_scrape %in% c("Ks") ~ "Kansas",
    region_scrape %in% c("Ky") ~ "Kentucky",
    region_scrape %in% c("La", "Baton Rouge", "New Orleans") ~ "Louisiana",
    region_scrape %in% c("Ma") ~ "Massachusetts",
    region_scrape %in% c("Md", "Baltimore") ~ "Maryland",
    region_scrape %in% c("Me", "Bangor") ~ "Maine",
    region_scrape %in% c("Allston, Ma", "Boston") ~ "Massachusetts",
    region_scrape %in% c("Mi") ~ "Michigan",
    region_scrape %in% c("Mn") ~ "Minnesota",
    region_scrape %in% c("Ms") ~ "Mississippi",
    region_scrape %in% c("Mo", "No") ~ "Missouri",
    region_scrape %in% c("Mt") ~ "Montana",
    region_scrape %in% c("Ne") ~ "Nebraska",
    region_scrape %in% c("Nv") ~ "Nevada",
    region_scrape %in% c("Nh", "NH") ~ "New Hampshire",
    region_scrape %in% c("Nm") ~ "New Mexico",
    region_scrape %in% c("Nj") ~ "New Jersey",
    region_scrape %in% c("New york", "Ny", "ny", "NY ", "Bronx", "Bronx ",
                         "Brooklyn", "Brooklyn ") ~ "New York",
    region_scrape %in% c("Oh", "Cincinnati", "Cleveland") ~ "Ohio",
    region_scrape %in% c("Ok", "ok") ~ "Oklahoma",
    region_scrape %in% c("Or") ~ "Oregon",
    region_scrape %in% c("Pa") ~ "Pennsylvania",
    region_scrape %in% c("Ri") ~ "Rhode Island",
    region_scrape %in% c("Sc", "sc", "Beaufort County") ~ "South Carolina",
    region_scrape %in% c("Tn", "tn", "Nashville") ~ "Tennessee",
    region_scrape %in% c("Tx", "Arlington", "Austin", "Dallas", "Houston") ~ 
      "Texas",
    region_scrape %in% c("Ut", "ut") ~ "Utah",
    region_scrape %in% c("Va") ~ "Virginia",
    region_scrape %in% c("Vt") ~ "Vermont",
    region_scrape %in% c("Wa") ~ "Washington",
    region_scrape %in% c("Wi") ~ "Wisconsin",
    region_scrape %in% c("Wy", "wy") ~ "Wyoming",
    TRUE ~ region_scrape
  ))


## Identify cases with agreement or with disagreement

US_agree <-
  US_comparison %>% 
  filter((region == region_geo & region == region_scrape) |
           (is.na(region_scrape) & region == region_geo))

US_no_agree <- 
  US_comparison %>% 
  filter(!property_ID %in% US_agree$property_ID)


## Intersect US_no_agree with 5 km buffer around state boundaries

US_no_agree_valid <- 
  property %>% 
  filter(property_ID %in% US_no_agree$property_ID) %>% 
  strr_as_sf(3857) %>% 
  st_intersection(st_buffer(states, 5000)) %>% 
  st_drop_geometry() %>% 
  select(property_ID, NAME) %>% 
  inner_join(select(US_no_agree, property_ID, NAME = region_scrape),
             by = c("property_ID", "NAME"))

US_no_agree <- 
  US_no_agree %>% 
  filter(!property_ID %in% US_no_agree_valid$property_ID)


## Accept geolocation result for remaining properties

US_finished <-
  US_agree %>% 
  select(property_ID, region, city) %>% 
  rbind(US_no_agree_valid %>% rename(region = NAME) %>% mutate(city = NA),
        US_no_agree %>% select(property_ID, region = region_geo, city))


### Bind results back into property table ######################################

property <-
  property %>% 
  filter(property_ID %in% US_finished$property_ID) %>% 
  select(-region, -city) %>% 
  inner_join(US_finished, by = "property_ID") %>% 
  select(property_ID:country, region, city, everything()) %>% 
  bind_rows(property %>% filter(!property_ID %in% US_finished$property_ID))


### Save temporary output ######################################################

temp_12_flag %<-% {
  save(property, file = "temp_12.Rdata")
  TRUE
}

rm(scrape_results, states, US_agree, US_comparison, US_finished, US_no_agree,
   US_no_agree_valid, US_states, state_names, US_nearest)


################################################################################
################################################################################



################################################################################
#### 7. FIX COUNTRIES WITH SPURIOUS REGION ENTRIES #############################

### Identify countries where fewer than 15% of listings have a region entry ####

region_whitelist <- 
  c("American Samoa", "Australia", "Belize", "Bolivia", "Brazil", "Canada", 
    "China", "Curaçao", "Egypt", "El Salvador", "Fiji", "France", "Georgia", 
    "Germany", "Ghana", "Guam", "Guatemala", "Honduras", "India", "Indonesia", 
    "Italy", "Japan", "Jordan", "Kosovo", "Malaysia", "Martinique", "Mauritius", 
    "Mexico", "Nepal", "New Zealand", "Nicaragua", "Northern Mariana Islands", 
    "Panama", "Philippines", "Portugal", "Puerto Rico", "Réunion", "Russia", 
    "Saint Barthélemy", "Saint Martin", "Senegal", "Sint Maarten", 
    "South Africa", "Spain", "Tanzania", "U.S. Virgin Islands", "Ukraine", 
    "United Kingdom", "United States", "Venezuela")

regions_to_check <- 
  property %>% 
  group_by(country) %>% 
  summarize(region_pct = mean(!is.na(region))) %>% 
  filter(region_pct > 0, !country %in% region_whitelist)

listings_to_check <- 
  property %>% 
  filter(country %in% regions_to_check$country, !is.na(region))


### Remove region from these countries

listings_to_check_finished <-
  listings_to_check %>%
  mutate(region = NA_character_,
         city = NA_character_)


### Bind results back into property table ######################################

property <-
  property %>% 
  filter(property_ID %in% listings_to_check_finished$property_ID) %>% 
  select(-country, -region, -city) %>% 
  inner_join(listings_to_check_finished, 
             by = c("property_ID", "host_ID", "listing_title", "property_type",
                    "listing_type", "created", "scraped", "housing", "latitude",
                    "longitude", "neighbourhood", "metro_area", "currency", 
                    "bedrooms", "bathrooms", "max_guests", "response_rate", 
                    "superhost", "premier_partner", "cancellation", 
                    "security_deposit", "cleaning_fee", "extra_people_fee",
                    "check_in_time", "check_out_time", "minimum_stay", 
                    "num_reviews", "num_photos", "instant_book", "rating", 
                    "ab_property", "ab_host", "ha_property", "ha_host")) %>% 
  select(property_ID:longitude, country, region, city, everything()) %>% 
  bind_rows(property %>% 
              filter(!property_ID %in% listings_to_check_finished$property_ID))


### Save temporary output ######################################################

temp_13_flag %<-% {
  save(property, file = "temp_13.Rdata")
  TRUE
}

rm(listings_to_check, listings_to_check_finished, regions_to_check,
   region_whitelist)


################################################################################
################################################################################



################################################################################
#### 8. DEAL WITH OTHER REGION ANOMALIES #######################################

### Harmonize Germany region names #############################################

Germany_fix <-
  property %>%
  filter(country == "Germany", region == "Hamburg") %>% 
  mutate(region = "Hamburg (Landmasse)")

if (nrow(Germany_fix) > 0) {
  property <-
    property %>% 
    filter(!property_ID %in% Germany_fix$property_ID) %>% 
    rbind(Germany_fix)
}


### Harmonize Russia region names ##############################################

Russia_fix <-
  property %>%
  filter(country == "Russia") %>% 
  mutate(region = case_when(
    region == "Dalnevostochnyj Federalnyj Okrug" ~ 
      "Far Eastern Federal District",
    region == "Sibirskij Federalnyj Okrug" ~ 
      "Siberian Federal District",
    region == "Privolzhskij Federalnyj Okrug" ~ 
      "Volga Federal District",
    region == "Severo Zapadnyj Federalnyj Okrug" ~ 
      "Northwestern Federal District",
    region == "Centralnyj Federal Okrug" ~ 
      "Central Federal District",
    region == "Severo Kavkazskij Federalnyj Okrug" ~ 
      "North Caucasian Federal District",
    region == "Uralskij Federalnyj Okrug" ~ 
      "Ural Federal District", 
    region == "Yuzhnyj Federalnyj Okrug" ~ 
      "Southern Federal District",
    TRUE ~ region
  ))

property <- 
  property %>% 
  filter(!property_ID %in% Russia_fix$property_ID) %>% 
  bind_rows(Russia_fix)


### Find countries with both valid and invalid region entries ##################

regions_with_many_countries <- 
  property %>% 
  filter(!is.na(region)) %>% 
  group_by(region) %>% 
  count(country) %>% 
  filter(n() >= 2) %>% 
  # Whitelist the regions that actually exist in two countries
  filter(!region %in% 
           c("Amazonas", "Distrito Federal", "Granada", "La Paz", "Leon"))


## Find countries with more or fewer entries of a region to identify problems

winners <- 
  regions_with_many_countries %>% 
  filter(n == max(n)) %>% 
  ungroup()

losers <- 
  regions_with_many_countries %>% 
  filter(n != max(n)) %>% 
  ungroup()

regions_many_property <- 
  map2_df(losers$region, losers$country, ~{
    property %>% filter(region == .x, country == .y)
  }) %>% 
  arrange(property_ID)


## Trust geolocation and purge region and city

regions_many_property <- 
  regions_many_property %>% 
  mutate(region = NA_character_,
         city = NA_character_)


### Bind results back into property table ######################################

property <-
  property %>% 
  filter(!property_ID %in% regions_many_property$property_ID) %>% 
  bind_rows(regions_many_property)


### Save temporary output ######################################################

temp_14_flag %<-% {
  save(property, file = "temp_14.Rdata")
  TRUE
}

rm(Germany_fix, Russia_fix, regions_with_many_countries, regions_many_property,
   winners, losers)


################################################################################
################################################################################



################################################################################
#### 9. FILL IN MISSING REGIONS USING CONVEX HULLS #############################

### Load existing region hulls

load("data/region_hulls.Rdata")


### Update convex hulls for all regions ########################################

region_hulls_2 <-
  property %>% 
  filter(!is.na(country), !is.na(region), 
         # Exclude Canada and US because they are handled separately
         country != "Canada", country != "United States") %>% 
  mutate(country_region = paste(country, region, sep = ", ")) %>% 
  filter(country_region %in% 
           (mutate(region_hulls, 
                   country_region = paste(country, region, sep = ", "))
           )$country_region) %>% 
  group_by(country, region) %>% 
  group_split() %>% 
  future_map(function(x) {
    x %>% 
      strr_as_sf(3857) %>% 
      group_by(country, region) %>% 
      summarize() %>% 
      mutate(geometry = st_union(geometry, filter(region_hulls, 
                                                  country == .$country, 
                                                  region == .$region))) %>% 
      st_convex_hull()
  }, .progress = TRUE)

region_hulls_2 <- 
  region_hulls_2 %>%
  data.table::rbindlist() %>% 
  as_tibble() %>% 
  st_as_sf() %>% 
  mutate(country_region = paste(country, region, sep = ", "))

region_hulls <-
  region_hulls %>% 
  mutate(country_region = paste(country, region, sep = ", ")) %>%
  filter(!country_region %in% region_hulls_2$country_region) %>% 
  rbind(region_hulls_2) %>% 
  select(-country_region)


### Add any needed new hulls ###################################################

hulls_to_add <- 
  property %>% 
  filter(!is.na(region),
         !region %in% region_hulls$region,
         !country %in% c("United States", "Canada")) %>% 
  count(country, region) %>% 
  filter(n >= 20) %>% 
  mutate(country_region = paste(country, region, sep = ", "))

if (nrow(hulls_to_add) > 0) {

  new_hulls <-
    property %>% 
    mutate(country_region = paste(country, region, sep = ", ")) %>%
    filter(country_region %in% hulls_to_add$country_region) %>% 
    select(-country_region) %>% 
    strr_as_sf(3857) %>% 
    group_by(country, region) %>% 
    summarize() %>% 
    st_convex_hull()

  region_hulls <- 
    rbind(region_hulls, new_hulls)
}


### Intersect listings without regions with the hull of their country ##########

country_groups <- 
  property %>% 
  filter(!is.na(country), is.na(region)) %>% 
  group_split(country)

region_intersects <- 
  country_groups %>% 
  future_map(function(x) {
    
    country_hulls <- region_hulls %>% filter(country == x[1,]$country)
    
    intersects <- 
      x %>% 
      strr_as_sf(3857) %>% 
      st_intersects(country_hulls)
    
    # For listings only intersecting one hull, assign them to that region
    map_chr(intersects, ~{
      if (length(.x) == 1) country_hulls[.x,]$region else NA_character_
    })
  }, .progress = TRUE) %>% 
  map2_df(country_groups, ~{mutate(.y, region = .x)})


### Bind results back into property table ######################################

property <-
  property %>% 
  filter(!property_ID %in% region_intersects$property_ID) %>% 
  bind_rows(region_intersects)


### Save updated region hulls and temporary output #############################

temp_15_flag %<-% {
  save(property, file = "temp_15.Rdata")
  TRUE
}

save(region_hulls, file = "data/region_hulls.Rdata")

suppressWarnings(
  rm(country_groups, region_hulls, region_hulls_2, region_intersects,
     hulls_to_add, new_hulls, temp_10_flag, temp_11_flag, temp_12_flag,
     temp_13_flag, temp_14_flag))



################################################################################
################################################################################



################################################################################
#### 10. FINALIZE PROPERTY TABLE AND LOCATION TABLE ############################

### Check property file for duplicates #########################################

if ({
  property %>% 
    count(property_ID) %>% 
    filter(n > 1) %>% 
    nrow() > 0}) {stop("Need to check additional location_table duplicates")}


### Merge property into master files ###########################################

load("temp_2.Rdata")

property <- 
  bind_rows(property_fixed, property)


### Produce final location_table ###############################################

location_table <- 
  property %>% 
  select(property_ID, latitude, longitude, country, region, city)


### Save output to disk and delete temporary files #############################

property_flag %<-% {
  save(property, 
       file = paste0("output/property/property_", year_month, ".Rdata"))
  TRUE
}

save(location_table, 
     file = paste0("output/property/location_table_", year_month, ".Rdata"))

if (property_flag) {
  file.remove("temp_1.Rdata", "temp_2.Rdata", "temp_3.Rdata", "temp_4.Rdata", 
              "temp_5.Rdata", "temp_6.Rdata", "temp_7.Rdata", "temp_8.Rdata",
              "temp_9.Rdata", "temp_10.Rdata", "temp_11.Rdata", "temp_12.Rdata",
              "temp_13.Rdata", "temp_14.Rdata", "temp_15.Rdata")
}

rm(location_table, property_fixed, property_flag, temp_15_flag)


################################################################################
################################################################################
