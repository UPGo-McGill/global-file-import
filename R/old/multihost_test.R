## Test for underlying ab_host entry for listings with host_ID == ha_host

ha_hosts <- 
  property %>% 
  filter(is.na(ab_host), !is.na(ha_host)) %>% 
  pull(host_ID) %>% 
  unique()

ha_hosts <- 
  property %>% 
  filter(!is.na(ab_host), ha_host %in% ha_hosts) %>% 
  pull(ha_host) %>% 
  unique()

ha_hosts_unique <- 
  property %>% 
  filter(ha_host %in% ha_hosts, !is.na(ab_host)) %>% 
  group_by(ha_host) %>% 
  summarize(ab_hosts = list(unique(ab_host))) %>% 
  filter(map(ab_hosts, length) == 1) %>% 
  pull(ha_host)

property %>% 
  filter(ha_host %in% ha_hosts_unique, is.na(ab_host))

property %>% 
  filter(ha_host == "30A-Escapes-245568b", ab_host != 91868695) %>% 
  select(property_ID, scraped, ab_property:ha_host)

property %>% 
  filter(ha_host == "Aadvisor-Rentals-640217w") %>% 
  select(property_ID, scraped, ab_property:ha_host)
