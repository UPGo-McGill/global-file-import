NS_points <- 
  property %>% 
  filter(country == "Canada", region == "Nova Scotia", !is.na(city)) %>% 
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>% 
  st_transform(3857) %>% 
  group_by(city) %>% 
  summarize()

NS_hulls <- 
  NS_points %>% 
  st_convex_hull() %>% 
  st_collection_extract("POLYGON")


NS_concave <- 
  NS_points %>% 
  concaveman(2) %>% 
  plot()



plot(NS_concave)

property %>% 
  filter(country == "Canada", region == "Nova Scotia", is.na(city)) %>% 
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>% 
  st_transform(3857) %>% 
  st_join(QC_hulls, left = FALSE)

property %>% 
  filter(property_ID == "ab-33261298") %>% view()


ggplot() + 
  geom_sf(data = filter(provinces, name == "Nova Scotia")) +
  geom_sf(data = NS_CSD) +
  geom_sf(data = NS_hulls, fill = "red", alpha = 0.1) +
  geom_sf(data = {property %>%
      filter(country == "Canada", region == "Nova Scotia", is.na(city)) %>%
      st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%
      st_transform(3857)}, alpha = 0.2) +
  # geom_sf(data = filter(st_intersection(NS_hulls), n.overlaps == 1), fill = "purple")
  theme_minimal()

TK %>% 
  st_intersection() %>% 
  filter(n.overlaps == 1) %>% 
  select(city, geometry)

CSD <- 
  cancensus::get_census("CA16", 
                      regions = list(C = "01"),
                      level = "CSD",
                      geo_format = "sf")


CA_city_match <- 
  property %>% 
  filter(country == "Canada", is.na(city)) %>% 
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%
  st_transform(3857) %>% 
  st_join(select(st_transform(CSD, 3857), name, geometry))


test3 <- 
CA_city_match %>% 
  mutate(name = str_extract(name, ".+(?= \\()"),
         name = str_replace(name, " No\\.", ""), 
         name = str_replace(name, " [:upper:]$", ""), 
         name = str_replace(name, " [:digit:]+$", "")) %>% 
  mutate(name = str_replace(name, ", .*$", "")) %>% 
  st_drop_geometry() %>%
  count(name)


cancensus::list_census_regions("CA16") %>% filter(PR_UID == "12", level == "CSD") %>% pull(region)





property %>% filter(country == "Canada", !is.na(city)) %>% 
  pull(city) %>% 
  unique()


CA_city_match %>% filter(is.na(name))



## Extract NS points with no city

NS_points <- 
  property %>%
  filter(country == "Canada", region == "Nova Scotia", is.na(city)) %>%
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%
  st_transform(3857)

NS_points <- 
  NS_points %>% 
  st_join(select(st_transform(NS_CSD, 3857), name, geometry))

NS_points %>% 
  st_drop_geometry() %>% 
  count(name) %>% view()

NS_points %>% 
  mutate(name = str_extract(name, "^[^\\(,\\d]*"),
         name = str_replace(name, "[:space:]$", "")) %>% 
  st_drop_geometry() %>% 
  count(name)


property %>% filter(country == "Canada", region == "Nova Scotia") %>% count(city) %>% view()






country_hulls <- 
  property %>% 
  filter(!is.na(country)) %>% 
  group_by(country) %>% 
  group_split() %>% 
  pbapply::pblapply(function(x) {
    x %>% 
      st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>% 
      st_transform(3857) %>% 
      group_by(country) %>% 
      summarize() %>% 
      st_convex_hull()  
  }, cl = 6)



country_hulls <- 
  country_hulls[map(country_hulls, ~{class(.x$geometry)}[[1]]) == "sfc_POLYGON"] %>% 
  data.table::rbindlist() %>% 
  as_tibble() %>% 
  st_as_sf()

country_hulls %>% 
  filter(!is.na(country)) %>% 
  ggplot() +
  geom_sf(aes(fill = country), alpha = 0.5) +
  theme_minimal()

country_hulls %>% 
  filter(!is.na(country)) %>% 
  mutate(area = st_area(geometry)) %>% 
  arrange(desc(area))

property %>% 
  filter(country == "Thailand", city == "Chiang Mai") %>% 
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>% 
  ggplot() +
  geom_sf()










