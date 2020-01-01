#### REBUILD COUNTRY TABLES ####################################################

### Load country data ##########################################################

load("data/countries.Rdata")


### Process Cyprus and Morocco #################################################

Cyprus_union <- 
  countries %>%
  filter(str_detect(name, "Cyprus") | name == "Akrotiri and Dhekelia") %>% 
  st_transform(3857) %>% 
  st_simplify(preserveTopology = TRUE, dTolerance = 50) %>%
  st_union()

Morocco_union <- 
  countries %>% 
  filter(name %in% c("Morocco", "Western Sahara")) %>% 
  st_transform(3857) %>% 
  st_simplify(preserveTopology = TRUE, dTolerance = 50) %>%
  st_union()


### Harmonize country names ####################################################

countries <- 
  countries %>% 
  filter(!name %in% c("Akrotiri and Dhekelia", "Antarctica",
                      "Bouvet Island", "British Indian Ocean Territory", 
                      "Caspian Sea", "Clipperton Island",
                      "French Southern Territories",
                      "Heard Island and McDonald Islands", "Iran", 
                      "North Korea", "Northern Cyprus", "Paracel Islands", 
                      "South Georgia and the South Sandwich Islands", 
                      "Spratly Islands", "Syria", "Tokelau",
                      "United States Minor Outlying Islands", "Vatican City",
                      "Western Sahara")
  ) %>% 
  mutate(name = as.character(name),
         name = case_when(
           name == "Åland"                ~ "Åland Islands",
           name == "Cocos Islands"        ~ "Cocos (Keeling) Islands",
           name == "Côte d'Ivoire"        ~ "Ivory Coast",
           name == "Falkland Islands"     ~ "Falkland Islands (Malvinas)",
           name == "Macao"                ~ "Macau",
           name == "Palestina"            ~ "Palestinian Territories",
           name == "Reunion"              ~ "Réunion",
           name == "Republic of Congo"    ~ "Congo",
           name == "Saint-Barthélemy"     ~ "Saint Barthélemy",
           name == "Saint-Martin"         ~ "Saint Martin",
           name == "Timor-Leste"          ~ "East Timor",
           name == "Virgin Islands, U.S." ~ "U.S. Virgin Islands",
           TRUE ~ name)) %>% 
  arrange(name) %>% 
  st_set_agr("identity") %>% 
  select(-code)


### Reduce complexity of polygons ##############################################

countries <- 
  countries %>% 
  st_transform(3857) %>% 
  st_simplify(preserveTopology = TRUE, dTolerance = 50)


### Substitute Cyprus and Morocco updated geometries ###########################

countries[countries$name == "Cyprus",]$geom <- Cyprus_union
countries[countries$name == "Morocco",]$geom <- Morocco_union

rm(Cyprus_union, Morocco_union)


### Produce subdivisions and 5 km buffers ######################################

countries_sub <- 
  countries %>% 
  lwgeom::st_subdivide(100) %>% 
  st_collection_extract("POLYGON")

country_buffer_list <- 
  countries %>% 
  group_split(name)

country_buffer_list <- 
  country_buffer_list[country_buffer_list %>% 
                        map_int(~{
                          .x %>% 
                            st_geometry() %>% 
                            st_cast("POLYGON") %>% 
                            st_cast("POINT") %>% 
                            length()
                        }) %>% 
                        order()]

country_buffer_list <- 
  country_buffer_list %>%
  pbapply::pblapply(st_buffer, 5000, cl = 6)

country_buffer_sub <- 
  country_buffer_list %>% 
  map(~{.x %>% 
      lwgeom::st_subdivide(100) %>% 
      st_collection_extract("POLYGON")}) %>% 
  data.table::rbindlist() %>% 
  as_tibble() %>% 
  st_as_sf()


### Create countries_2 from rnaturalearth boundaries ###########################

countries_2 <- 
  rnaturalearthhires::countries10 %>% 
  st_as_sf() %>% 
  as_tibble() %>% 
  st_as_sf() %>% 
  st_transform(3857) %>% 
  select(name = NAME, code = ADM0_A3) %>% 
  mutate(name = case_when(
    name == "Åland"                    ~ "Åland Islands",
    name == "Antigua and Barb."        ~ "Antigua and Barbuda",
    # name == ???                      ~ Bonaire, Sint Eustatius and Saba,
    name == "Bosnia and Herz."         ~ "Bosnia and Herzegovina",
    name == "British Virgin Is."       ~ "British Virgin Islands",
    name == "Cabo Verde"               ~ "Cape Verde",
    name == "Cayman Is."               ~ "Cayman Islands",
    name == "Central African Rep."     ~ "Central African Republic",
    ## name == ???                     ~ "Christmas Island",
    ## name == ???                     ~ "Cocos Islands",
    name == "Cook Is."                 ~ "Cook Islands",
    name == "Czechia"                  ~ "Czech Republic",
    name == "Dem. Rep. Congo"          ~ "Democratic Republic of the Congo",
    name == "Dominican Rep."           ~ "Dominican Republic",
    name == "Eq. Guinea"               ~ "Equatorial Guinea",
    name == "eSwatini"                 ~ "Swaziland",
    name == "Falkland Is."             ~ "Falkland Islands (Malvinas)",
    name == "Faeroe Is."               ~ "Faroe Islands",
    ## name == ???                     ~ "French Guiana",
    name == "Fr. Polynesia"            ~ "French Polynesia",
    ## name == ???                     ~ "Guadeloupe",
    name == "Macao"                    ~ "Macau",
    name == "Marshall Is."             ~ "Marshall Islands",
    ## name == ???                     ~ "Martinique",
    ## name == ???                     ~ "Mayotte",
    name == "N. Mariana Is."           ~ "Northern Mariana Islands",
    name == "Palestine"                ~ "Palestinian Territories",
    name == "Pitcairn Is."             ~ "Pitcairn Islands",
    ## name == ???                     ~ "Réunion",
    name == "St-Barthélemy"            ~ "Saint Barthélemy",
    name == "St. Kitts and Nevis"      ~ "Saint Kitts and Nevis",
    name == "St. Pierre and Miquelon"  ~ "Saint Pierre and Miquelon",
    name == "St. Vin. and Gren."       ~ "Saint Vincent and the Grenadines",
    name == "St-Martin"                ~ "Saint Martin",
    name == "São Tomé and Principe"    ~ "São Tomé and Príncipe",
    name == "Solomon Is."              ~ "Solomon Islands",
    name == "S. Sudan"                 ~ "South Sudan",
    name == "Timor-Leste"              ~ "East Timor",
    name == "Turks and Caicos Is."     ~ "Turks and Caicos Islands",
    name == "U.S. Virgin Is."          ~ "U.S. Virgin Islands",
    name == "United States of America" ~ "United States",
    name == "Wallis and Futuna Is."    ~ "Wallis and Futuna",
    name == "W. Sahara"                ~ "Western Sahara",
    TRUE ~ name
  ))

Cyprus_union <-
  countries_2 %>%
  filter(str_detect(name, "Cyprus|Akrotiri|Dhekelia")) %>% 
  st_union()

Morocco_union <- 
  countries_2 %>% 
  filter(name %in% c("Morocco", "Western Sahara")) %>% 
  st_union()
  
countries_2[countries_2$name == "Cyprus",]$geometry <- Cyprus_union
countries_2[countries_2$name == "Morocco",]$geometry <- Morocco_union

rm(Cyprus_union, Morocco_union)

countries_2 <- 
  countries_2 %>% 
  # Remove Macau because of inaccurate boundaries
  filter(name != "Macau") %>% 
  inner_join(st_drop_geometry(countries)) %>% 
  select(-code)

countries_2_sub <- 
  countries_2 %>% 
  lwgeom::st_subdivide(100) %>% 
  st_collection_extract("POLYGON")

country_2_buffer_list <- 
  countries_2 %>% 
  group_split(name)

country_2_buffer_list <- 
  country_2_buffer_list[country_2_buffer_list %>% 
                          map_int(~{
                            .x %>% 
                              st_geometry() %>% 
                              st_cast("POLYGON") %>% 
                              st_cast("POINT") %>% 
                              length()
                          }) %>% 
                          order()]

country_2_buffer_list <- 
  country_2_buffer_list %>%
  pbapply::pblapply(st_buffer, 5000, cl = 6)

country_2_buffer_sub <- 
  country_2_buffer_list %>% 
  map(~{.x %>% 
      lwgeom::st_subdivide(100) %>% 
      st_collection_extract("POLYGON")}) %>% 
  data.table::rbindlist() %>% 
  as_tibble() %>% 
  st_as_sf()


### Set attributes and save output #############################################

countries <- st_set_agr(countries, "identity")
countries_2 <- st_set_agr(countries_2, "identity")
countries_2_sub <- st_set_agr(countries_2_sub, "identity")
countries_sub <- st_set_agr(countries_sub, "identity")
country_2_buffer_list <- map(country_2_buffer_list, st_set_agr, "identity")
country_2_buffer_sub <- st_set_agr(country_2_buffer_sub, "identity")
country_buffer_list <- map(country_buffer_list, st_set_agr, "identity")
country_buffer_sub <- st_set_agr(country_buffer_sub, "identity")

save(countries, countries_2, countries_2_sub, countries_sub, 
     country_2_buffer_list, country_2_buffer_sub, country_buffer_list, 
     country_buffer_sub, file = "data/country_boundaries.Rdata")



### Rebuild region hulls #######################################################

### TKTK


################################################################################
################################################################################


