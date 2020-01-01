################################################################################
############################### MONTHLY SCRAPER ################################
################################################################################



################################################################################
#### 1. LOAD LIBRARIES #########################################################

library(tidyverse)
library(upgo)


################################################################################
################################################################################



################################################################################
#### 2. SCRAPE HIGH-PRIORITY LISTINGS ##########################################

### Load scrape pool and initialize server #####################################

load("data/scrape_pool.Rdata")
load("data/scrape_results.Rdata")

new_scrape_1 <- scrape_results[0,]

rD <- upgo_scrape_connect()


### While loop to perform scrape incrementally #################################

n <- 1

while (nrow(filter(scrape_pool_1, 
                   !property_ID %in% scrape_results$property_ID,
                   !property_ID %in% new_scrape_1$property_ID)) > 0 & 
       n < 200) {
  
  n <- n + 1
  
  scrape_no_matches <- 
    scrape_pool_1 %>% 
    filter(!property_ID %in% scrape_results$property_ID,
           !property_ID %in% new_scrape_1$property_ID) %>% 
    dplyr::slice(1:10000) %>% 
    upgo_scrape_location(cores = 4)
  
  new_scrape_1 <- 
    new_scrape_1 %>% 
    rbind(scrape_no_matches) %>% 
    filter(!is.na(raw)) %>% 
    distinct()
  
  save(new_scrape_1, file = "data/new_scrape_1.Rdata")  
  
}

new_scrape_1 <- 
  new_scrape_1 %>% 
  rbind(.temp_results) %>% 
  filter(!is.na(raw)) %>% 
  distinct()


### Try again for listings with non-conforming country entries #################

load("data/country_boundaries.Rdata")

try_again <- 
  new_scrape_1 %>% 
  filter(!is.na(country), !country %in% countries$name) %>% 
  upgo_scrape_location(cores = 4)

new_scrape_1 <- 
  new_scrape_1 %>% 
  filter(!property_ID %in% try_again$property_ID) %>% 
  rbind(try_again)


### Clean up ###################################################################

upgo_scrape_disconnect()

scrape_results <- 
  scrape_results %>% 
  filter(!property_ID %in% new_scrape_1$property_ID) %>% 
  rbind(new_scrape_1)

scrape_results <- 
  scrape_results %>% 
  filter(!is.na(raw)) %>% 
  distinct() %>% 
  group_by(property_ID) %>% 
  filter(date == max(date)) %>% 
  ungroup()

save(scrape_results, file = "data/scrape_results.Rdata")  

file.remove("data/new_scrape_1.Rdata")

rm(scrape_no_matches, new_scrape_1, countries, countries_2, countries_2_sub,
   countries_sub, country_2_buffer_list, country_2_buffer_sub,
   country_buffer_list, country_buffer_sub, try_again, scrape_pool_1,
   scrape_pool_2, scrape_results)


################################################################################
################################################################################



################################################################################
#### 3. SCRAPE LOW-PRIORITY LISTINGS ###########################################

### Load scrape pool and initialize server #####################################

load("data/scrape_pool.Rdata")
load("data/scrape_results.Rdata")

new_scrape_2 <- scrape_results[0,]

rD <- upgo_scrape_connect()


### While loop to perform scrape incrementally #################################

n <- 1

while (nrow(filter(scrape_pool_2, 
                   !property_ID %in% scrape_results$property_ID,
                   !property_ID %in% new_scrape_2$property_ID)) > 0 & 
       n < 200) {
  
  n <- n + 1
  
  scrape_no_matches <- 
    scrape_pool_2 %>% 
    filter(!property_ID %in% scrape_results$property_ID,
           !property_ID %in% new_scrape_2$property_ID) %>% 
    dplyr::slice(1:10000) %>% 
    upgo_scrape_location(cores = 4)
  
  new_scrape_2 <- 
    new_scrape_2 %>% 
    rbind(scrape_no_matches) %>% 
    filter(!is.na(raw)) %>% 
    distinct()
  
  save(new_scrape_2, file = "data/new_scrape_2.Rdata")  
  
}

new_scrape_2 <- 
  new_scrape_2 %>% 
  rbind(.temp_results) %>% 
  filter(!is.na(raw)) %>% 
  distinct()


### Try again for listings with non-conforming country entries #################

load("data/country_boundaries.Rdata")

try_again <-
  new_scrape_2 %>% 
  filter(!is.na(country), !country %in% countries$name) %>% 
  upgo_scrape_location(cores = 4)

new_scrape_2 <- 
  new_scrape_2 %>% 
  filter(!property_ID %in% try_again$property_ID) %>% 
  rbind(try_again)


### Clean up ###################################################################

upgo_scrape_disconnect()

scrape_results <- 
  scrape_results %>% 
  filter(!property_ID %in% new_scrape_2$property_ID) %>% 
  rbind(new_scrape_2)

scrape_results <- 
  scrape_results %>% 
  filter(!is.na(raw)) %>% 
  distinct() %>% 
  group_by(property_ID) %>% 
  filter(date == max(date)) %>% 
  ungroup()

save(scrape_results, file = "data/scrape_results.Rdata")  

file.remove("data/new_scrape_2.Rdata", "data/scrape_pool.Rdata")

rm(scrape_no_matches, new_scrape_2, countries, countries_2, countries_2_sub,
   countries_sub, country_2_buffer_list, country_2_buffer_sub,
   country_buffer_list, country_buffer_sub, try_again, scrape_pool_1,
   scrape_pool_2, scrape_results)


################################################################################
################################################################################
