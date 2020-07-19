library(ghql)
library(tidyverse)
library(jsonlite)
library(glue)
library(progress)
library(lubridate)
library(ggplot2)
library(sf)

setwd("GitHub/traffic-covid19/")
munics <- read_sf("Kommuner.geojson")

con <- GraphqlClient$new(
  url = "https://www.vegvesen.no/trafikkdata/api/"
)

qry <- Query$new()

qry$query("regpoints", '{
  trafficRegistrationPoints(searchQuery: { roadCategoryIds: [E] }) {
    id
    name
    trafficRegistrationType
    direction {
      from
      to
    }
    location {
      coordinates {
        latLon {
          lat
          lon
        }
      }
    }
  }
}')

regpoints <- con$exec(qry$queries$regpoints) %>%
  fromJSON() %>%
  flatten_df() %>%
  bind_cols(.$direction, .$location$coordinates$latLon) %>%
  select(-direction, -location)

regpoint_ids <- regpoints$id

# 2019
date2019_from <- "2019-07-08T00:00:00+02:00"
date2019_to <- "2019-07-12T00:00:00+02:00"

hourly_2019_list <- vector(mode = "list", length = length(regpoint_ids))
names(hourly_2019_list) <- regpoint_ids

pb <- progress_bar$new(total = length(regpoint_ids))

for (regpoint in regpoint_ids) {
  pb$tick()
  qry$query("hourly_2019", glue(.open = "<", .close = ">", 
  '{
    trafficData(trafficRegistrationPointId: <double_quote(regpoint)>) {
      volume {
        byHour(
          from: <double_quote(date2019_from)>
          to: <double_quote(date2019_to)>
        ) {
          edges {
            node {
              from
              to
              total {
                volumeNumbers {
                  volume
                }
                coverage {
                  percentage
                }
              }
            }
          }
        }
      }
    }
  }'))
  
  hourly <- con$exec(qry$queries$hourly_2019) %>%
    fromJSON()
  trafficdata <- hourly$data$trafficData$volume$byHour$edges$node$total %>%
    flatten_df()
    
  trafficdata$id <- regpoint
  trafficdata$from_time <- hourly$data$trafficData$volume$byHour$edges$node$from
  trafficdata$to_time <- hourly$data$trafficData$volume$byHour$edges$node$to
  hourly_2019_list[[regpoint]] <- trafficdata
}

# 2020
date2020_from <- "2020-07-06T00:00:00+02:00"
date2020_to <- "2020-07-10T00:00:00+02:00"

hourly_2020_list <- vector(mode = "list", length = length(regpoint_ids))
names(hourly_2020_list) <- regpoint_ids

pb <- progress_bar$new(total = length(regpoint_ids))

for (regpoint in regpoint_ids) {
  pb$tick()
  qry$query("hourly_2020", glue(.open = "<", .close = ">", 
  '{
    trafficData(trafficRegistrationPointId: <double_quote(regpoint)>) {
      volume {
        byHour(
          from: <double_quote(date2020_from)>
          to: <double_quote(date2020_to)>
        ) {
          edges {
            node {
              from
              to
              total {
                volumeNumbers {
                  volume
                }
                coverage {
                  percentage
                }
              }
            }
          }
        }
      }
    }
  }'))
  
  regpoint_data <- con$exec(qry$queries$hourly_2020) %>%
    fromJSON()
  trafficdata <- regpoint_data$data$trafficData$volume$byHour$edges$node$total %>%
    flatten_df()
  
  trafficdata$id <- regpoint
  trafficdata$from_time <- regpoint_data$data$trafficData$volume$byHour$edges$node$from
  trafficdata$to_time <- regpoint_data$data$trafficData$volume$byHour$edges$node$to
  hourly_2020_list[[regpoint]] <- trafficdata
}

hourly_2019 <- do.call("rbind", hourly_2019_list)
hourly_2020 <- do.call("rbind", hourly_2020_list)

hourly <- rbind(hourly_2019, hourly_2020) %>%
  mutate(from_time = as_datetime(from_time, tz = "Europe/Oslo"),
         to_time = as_datetime(to_time, tz = "Europe/Oslo"),
         hour = hour(from_time),
         day = day(from_time),
         year = year(from_time))

hourly_wide <- hourly %>%
  group_by(id, year) %>%
  summarize(volume = sum(volume)) %>%
  ungroup() %>%
  pivot_wider(names_from = year, values_from = volume, names_prefix = "volume") %>%
  left_join(regpoints, by = "id") %>%
  st_as_sf(coords = c("lon", "lat"), crs = 4326) %>%
  st_transform(25833)

hourly %>%
  group_by(hour, year) %>%
  summarize(volume = mean(volume)) %>%
  ggplot(aes(fill = factor(year), x = hour, y = volume)) +
  geom_bar(stat = "identity")
  
hourly_munics <- st_join(munics, hourly_wide) %>%
  group_by(kommunenummer) %>%
  summarize(volume2020 = mean(volume2020),
            volume2019 = mean(volume2019)) %>%
  mutate(diff_2020_2019 = volume2020 - volume2019)
  

