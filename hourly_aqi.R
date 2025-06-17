#O3 census tract data

#Packages
##lubridate - package with added date-time capabilities beyond base R
##tidyverse - collection of R packages designed for data science
##raster - has functions for creating, reading, manipulating, and writing raster data
##parallel - support for parallel computation
##sf - supports simple features, a standardized way to encode spatial vector data


library(lubridate)
library(tidyverse) #dplyr, ggplot2, stringr 
library(parallel)
library(abind)
install.packages("terra")
library(terra)  
library(sp)
library(stars) #sf
library(tigris)
library(exactextractr)
library(raster)

#Dates dataframe
#building dataframe to adhere to specific date forms needed to retrieve data from AirNow API
# Ex: https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/2024/20240210/US-24021006.grib2
#Website link: https://files.airnowtech.org/?prefix=airnow/

#Changing to only pull for last hour not 48

current_date <- Sys.Date() #as UTC
current_time <- Sys.time() #as EDT

#date_1 <- current_time-(1*60*60) don't remember why this was here

top_of_hour <- floor_date(current_time, "hour") #sets back to previous top of hour
top_of_previous_hour <- top_of_hour - (3600*2)
#multiplying by 2 to test whether pushing back the file pull date improves pipeline success
###previously was running into errors where file push was not happening soon enough by AirNow

#used to pull 48 hours at a time. Too slow but if need be just add here (see original code)
datetimes <- as_datetime(top_of_previous_hour)
datetimes_utc <- format(datetimes, tz="UTC",usetz = TRUE)
#AirNow dates are in UTC

datetimes_df <- data.frame(datetimes_utc)

datetimes_df$date <- as.Date(datetimes_df$datetimes_utc)
datetimes_df$time <- format(as.POSIXct(datetimes_df$datetimes_utc),
                            format = "%H:%M:%S")

#pulling 'hours' from time column
datetimes_df$time_0 <- gsub(":","",datetimes_df$time)
datetimes_df$hours <- stringr::str_sub(datetimes_df$time_0, start = 1, end = 2)

#making dateformat 1, 2 and years columns to match URL syntax
datetimes_df$date_format1 <- gsub("-","",datetimes_df$date)
datetimes_df$date_format2 <- stringr::str_sub(datetimes_df$date_format1, start = 3)
datetimes_df$year <- stringr::str_sub(datetimes_df$date_format1, end = 4)

#Downloading data from each of the API links from the previous 1 hour

#Need to see if it makes more sense to keep lapply/function in for scalability
##or just paste link directly


############O3 data#####################################3
dat_download <-

  lapply(1:nrow(datetimes_df),
         FUN = function(x){
           
           URL <- 
             paste0("https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/",
                    datetimes_df$year[x],
                    "/",
                    datetimes_df$date_format1[x],
                    "/US-",
                    datetimes_df$date_format2[x],
                    datetimes_df$hours[x],
                    ".grib2")
           return(URL)
           
         })


urlchar <- as.character(dat_download)

#Transforming raster data -- read_stars to read raster/array data
spatdata <- read_stars(urlchar)
rastdata <- rast(spatdata)

#Bringing in county files and extract using exactextractr package
counties <- counties()
terra1 <- exact_extract(rastdata, counties, 
                        include_cols = c("STATEFP", "COUNTYFP", "GEOID", "NAMELSAD"))

#value: value of the intersecting raster cells
#coverage_fraction: fraction of intersecting area relative to full raster grid
#  -- can help find coverage-weighted summary of extracted values

#Note that id value of i represents ith element of the list, 
#which in turn corresponds to ith polygon in the polygons (counties) data


#flat table
spat_table <- dplyr::bind_rows(terra1)


#pivot table to create a datetime column

spat_table_long <- pivot_longer(
  spat_table,
  -c(GEOID, STATEFP, NAMELSAD, COUNTYFP,coverage_fraction),
  names_to = "datetime",
  values_to = "value"
)

#Adding the values to the 'datetime' column
spat_table_long$datetime <- datetimes_df$datetimes_utc
spat_table_long$weight <- as.numeric(spat_table_long$coverage_fraction)

#grouped by GEOID and date

spat_table_long_2 <-
  spat_table_long %>%
  group_by(GEOID, datetime) %>%
  
  summarize(o3_prop_area = sum(weight[which(!is.na(value))])/sum(weight),
#first part of summarize is figuring out what proportion of each county has data available
            o3_AQI = ifelse(o3_prop_area <.75, NA, weighted.mean(na.rm=T, w=weight,x=value))) 
#second part calculates weighted average if there's at least 75% coverage otherwise NA is reported

spat_table_long_3 <- spat_table_long_2 %>%
  mutate(o3_AQI_category = case_when(
    o3_AQI <= 50 ~ 1, #Good
    o3_AQI > 50 & o3_AQI <= 100 ~ 2, #Moderate
    o3_AQI > 100 & o3_AQI <= 150 ~ 3, #Unhealthy for sensitive groups
    o3_AQI > 150 & o3_AQI <= 200 ~ 4, #Unhealthy
    o3_AQI > 200 & o3_AQI <= 300 ~ 5, #Very Unhealthy
    o3_AQI > 300 ~ 6 #Hazardous
  ))

#converting datetime values from chr to POSIXCT
spat_table_long_3$datetime <- with_tz(as.POSIXct(spat_table_long_3$datetime, tz = "UTC"), tzone = "America/New_York")


dat_download <-
  
  lapply(1:nrow(datetimes_df),
         FUN = function(x){
           
           URL <- 
             paste0("https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/",
                    datetimes_df$year[x],
                    "/",
                    datetimes_df$date_format1[x],
                    "/US-",
                    datetimes_df$date_format2[x],
                    datetimes_df$hours[x],
                    ".grib2")
           return(URL)
           
         })


urlchar <- as.character(dat_download)

#Transforming raster data -- read_stars to read raster/array data
spatdata <- read_stars(urlchar)
rastdata <- rast(spatdata)

#Bringing in county files and extract using exactextractr package
counties <- counties()
terra1 <- exact_extract(rastdata, counties, 
                        include_cols = c("STATEFP", "COUNTYFP", "GEOID", "NAMELSAD"))

#value: value of the intersecting raster cells
#coverage_fraction: fraction of intersecting area relative to full raster grid
#  -- can help find coverage-weighted summary of extracted values

#Note that id value of i represents ith element of the list, 
#which in turn corresponds to ith polygon in the polygons (counties) data


#flat table
spat_table <- dplyr::bind_rows(terra1)


#pivot table to create a datetime column

spat_table_long <- pivot_longer(
  spat_table,
  -c(GEOID, STATEFP, NAMELSAD, COUNTYFP,coverage_fraction),
  names_to = "datetime",
  values_to = "value"
)

#Adding the values to the 'datetime' column
spat_table_long$datetime <- datetimes_df$datetimes_utc
spat_table_long$weight <- as.numeric(spat_table_long$coverage_fraction)

#grouped by GEOID and date

spat_table_long_2 <-
  spat_table_long %>%
  group_by(GEOID, datetime) %>%
  
  summarize(o3_prop_area = sum(weight[which(!is.na(value))])/sum(weight),
            #first part of summarize is figuring out what proportion of each county has data available
            o3_AQI = ifelse(o3_prop_area <.75, NA, weighted.mean(na.rm=T, w=weight,x=value))) 
#second part calculates weighted average if there's at least 75% coverage otherwise NA is reported

spat_table_long_3 <- spat_table_long_2 %>%
  mutate(o3_AQI_category = case_when(
    o3_AQI <= 50 ~ 1, #Good
    o3_AQI > 50 & o3_AQI <= 100 ~ 2, #Moderate
    o3_AQI > 100 & o3_AQI <= 150 ~ 3, #Unhealthy for sensitive groups
    o3_AQI > 150 & o3_AQI <= 200 ~ 4, #Unhealthy
    o3_AQI > 200 & o3_AQI <= 300 ~ 5, #Very Unhealthy
    o3_AQI > 300 ~ 6 #Hazardous
  ))

#converting datetime values from chr to POSIXCT
spat_table_long_3$datetime <- with_tz(as.POSIXct(spat_table_long_3$datetime, tz = "UTC"), tzone = "America/New_York")
#table values are not for visualization


#######################VISUALIZATION###########################################
install.packages("viridis")
library(viridis)

#plot
plot(rastdata)

#plot with title
rast_plot <- plot(rastdata[[1]], col = viridis::viridis(100), main = names(rastdata))


#Overlaying with county map

#transforming coordinate reference system so counties file matches raster data
us_counties <- st_transform(counties, crs(rastdata))
rast_df <- as.data.frame(rastdata, xy = TRUE, na.rm = TRUE)
names(rast_df)[3] <- "AQI"

ggplot() + 
  geom_raster(data = rast_df, aes(x=x, y=y, fill = AQI)) +
  scale_fill_viridis_c(option = "C", name = "AQI") +
  geom_sf(data = counties, fill=NA, color="gray", size = 0.1) +
  coord_sf(xlim = c(-125, -66), ylim = c(24, 50)) +
  theme_minimal() +
  labs(title = "Contiguous US hourly Air Quality Index (AQI) map",
       caption = paste0("AQI overlaid with US counties: ", datetimes_df$date, datetimes_df$time)) 




