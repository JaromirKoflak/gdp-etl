setwd("C:/Users/jaromir.koflak/OneDrive - United Nations/Team Olomouc/GDP")

library(wbstats)
library(readr)
library(ggplot2)

datadir = paste(getwd(), "/data", sep = "")
date_today <- format(Sys.Date(), "%Y%m%d")
if (!dir.exists(file.path(datadir, date_today))) dir.create(file.path(datadir, date_today))
yy <- as.integer(format(Sys.Date(), "%Y"))
mm <- format(Sys.Date(), "%m")
dat = date_today
FirstYear = 1990
LastYear = 2025

# read_data <- function(datadir, dat, FirstYear, LastYear) {

# Description -------------------------------------------------------------
  # Download data for compilation of the Inclusive Growth Index:
  # Pillar 1
  #  1.1 - GDP per capita, PPP (constant 2017 international $)
  #  1.2 - Adjusted net national income per capita (constant 2015 US$)
  #  1.3 - Labour productivity - GDP per person employed (constant 2017 PPP USD)
  #  1.4 - Employment to population ratio, 15+, total (%) (modeled ILO estimate)
  #  1.5 - Electricity consumption/population (kWh per capita)
  #  1.6 - Exports of goods and services (% of GDP)
  # Pillar 2
  #  2.1 - Logistics performance index: Overall (1=low to 5=high)
  #  2.2 - Fixed broadband subscriptions per 100 inhabitants, by speed (per 100 inhabitants)
  #  2.3 - Under-five mortality rate, by sex (deaths per 1,000 live births)
  #  2.4 - Proportion of population using safely managed drinking water services, by urban/rural (%)
  #  2.5 - School enrollment, secondary (% gross)
  #  2.6 - Universal health coverage (UHC) service coverage index
  #  2.7 - Proportion of adults (15 years and older) with an account at a financial institution or mobile-money-service provider, by sex (% of adults aged 15 years and older)
  # Pillar 3
  # 3.1 - Gini index
  # 3.2 - Poverty headcount ratio at $3.65 a day (2017 PPP) (% of population)
  # 3.3 - School enrolment, secondary (gross), gender parity index (GPI)
  # 3.4 - Ratio of female to male labor force participation rate (%) (modeled ILO estimate) - same ad 3.7
  # 3.5 - YUR / AUR, ILOEST_ Youth unemployment rate and adult unemployment rate by sex (ILO modelled estimates)
  # 3.6 - Proportion of seats held by women in national parliaments (% of total number of seats)
  # 3.7 - Ratio of female to male labour force participation rate (%) (modeled ILO estimate)
  # 3.8 - Ratio of female age of first marriage to male age of first marriage
  # 3.9 - Ratio of the share of wage and salaried workers in women’s to men’s employment
  # 3.10 - Share of women's service employment to total employment, raised to the power of the inverse of the Palma ratio
  # Pillar 4
  # 4.1 - Carbon dioxide emissions per unit of GDP PPP (kilogrammes of CO2 per constant 2017 United States dollars)
  # 4.2 - Energy intensity level of primary energy (megajoules per constant 2017 purchasing power parity GDP)
  # 4.3 - Water Use Efficiency (United States dollars per cubic meter)
  # 4.4 - Terrestrial biodiversity area as % total protected areas
  #
  # It reads all files necessary and saves them in "datadir" on the date "dat".
  # Data are not processed at this stage, it is a raw data download.
  #


# Settings ----------------------------------------------------------------

  # date_today <- format(Sys.Date(), "%Y%m%d")
  # if (!dir.exists(file.path(datadir, date_today))) dir.create(file.path(datadir, date_today))
  # yy <- as.integer(format(Sys.Date(), "%Y"))
  # mm <- format(Sys.Date(), "%m")
  

# Source: World Bank ------------------------------------------------------
## using the wbstats package
  wb_indicators <- c("NY.GDP.PCAP.PP.KD", #GDP per capita, PPP %>% (constant 2017 international $)
                      "NY.ADJ.NNTY.PC.KD", #Adjusted net national income per capita (constant 2015 US$)
                      "SL.GDP.PCAP.EM.KD", #GDP per person employed (constant 2017 PPP $)
                      "NE.EXP.GNFS.ZS", #Exports of goods and services (% of GDP)
                      "LP.LPI.OVRL.XQ", #Logistics performance index: Overall (1=low to 5=high)
                      #"SE.SEC.ENRR", #School enrollment, secondary (% gross)
                     # removed due to inconsistency with UNESCO data
                     "SI.POV.GINI", #Gini index
                     #"SI.POV.LMIC", #Poverty headcount ratio at $3.65 a day (2017 PPP) (% of population)
                     # Estimates from the World Bank's Poverty and Inequality Platform (PIP) used instead
                     #"SE.ENR.SECO.FM.ZS", #School enrollment, secondary (gross), gender parity index (GPI)
                     # removed due to inconsistency with UNESCO data
                     # new indicator "adjusted gender parity index" is now produced
                     "SL.TLF.CACT.FM.ZS", #Ratio of female to male labor force participation rate (%) (modeled ILO estimate)
                     "SL.EMP.TOTL.SP.ZS", #Employment to population ratio, 15+, total (%) (modeled ILO estimate)
                     "SL.EMP.TOTL.SP.FE.ZS", #Employment to population ratio, 15+, female (%) (modeled ILO estimate)
                     "SL.EMP.TOTL.SP.MA.ZS", #Employment to population ratio, 15+, male (%) (modeled ILO estimate)
                     "SL.EMP.WORK.FE.ZS", #Wage and salaried workers, female (% of female employment) (modeled ILO estimate)
                     "SL.EMP.WORK.MA.ZS", #Wage and salaried workers, male (% of male employment) (modeled ILO estimate)
                     "SL.SRV.EMPL.FE.ZS",  #Employment in services, female (% of female employment) (modeled ILO estimate)
                     "SL.TLF.TOTL.FE.ZS",  #Labor force, female (% of total labor force)
                     "SH.H2O.BASW.ZS", #People using at least basic drinking water services (% of population)
                     "ER.GDP.FWTL.M3.KD" #Water productivity, total (constant 2015 US$ GDP per cubic meter of total freshwater withdrawal)
                     )
  wbdf <- wb_data(wb_indicators, start_date=FirstYear, end_date=LastYear)
  write_csv(wbdf, file.path(datadir, dat, paste0("WB_data_raw.csv")))
  
# Source: ILOSTAT ---------------------------------------------------------
## using the rilostat package
  ilo_indicators <- c("EMP_2WAP_SEX_AGE_RT_A" #Employment-to-population ratio by sex and age -- ILO modelled estimates, Nov. 2022 (%) -- Annual
                      ) #so far majority of employment indicators are sourced from the World Bank due to simplicity 
                        # but a prefered source would be directly ILOSTAT as the main data producer
  # Youth -> "Age (Youth bands): 15-24"=="AGE_YTHADULT_Y15-24"
  # Adults -> "Age (Youth, adults): 15+"=="AGE_YTHADULT_YGE25"
  ilodf <- get_ilostat(id=ilo_indicators) %>%
    filter(sex=="SEX_T",
           classif1 %in% c("AGE_YTHADULT_YGE25","AGE_YTHADULT_Y15-24"),
           time %in% c(FirstYear:LastYear))
  data_lab <- label_ilostat(ilodf)
  write_csv(ilodf, file.path(datadir, dat, "ILO_data_raw.csv"))

# Source: SDG Global Database ---------------------------------------------
  # Using Fernando's R script for now (it may be simplified)
  # Download list of targeted series for SDG indicators 
  url <- "https://unstats.un.org/sdgs/UNSDGAPIV5/v1/sdg/Indicator/List"
  rawdata <- fromJSON(url)
  series <- rawdata %>%
    as_tibble() %>%
    filter(goal %in% c("3", "5", "6", "7", "8", "9", "15", "17")) %>%
    filter(code %in% c("3.2.1", 
                       "3.8.1",
                       "5.5.1",
                       "6.1.1", 
                       "6.4.1",
                       "7.3.1",
                       "8.10.2",
                       "9.4.1",
                       "15.1.2",
                       "17.6.1"
                       ))
  series <- do.call(rbind, series$series) %>% as_tibble()
  for (i in 1:nrow(series)){
    temp1 <- flatten(series[i, "goal"])$goal[[1]]
    temp2 <- flatten(series[i, "target"])$target[[1]]
    temp3 <- flatten(series[i, "indicator"])$indicator[[1]]
    if (length(temp1) == 1) {
      series[i, "goal2"] <- temp1
      series[i, "target2"] <- temp2
      series[i, "indicator2"] <- temp3
    } else {
      series[i, "goal2"] <- temp1[temp1 %in% c("3", "5", "6", "7", "8", "9", "15", "17")]
      series[i, "target2"] <- temp2[substr(temp2, 1, 1) %in% c("3", "5", "6", "7", "8", "9", "15", "17")]
      series[i, "indicator2"] <- temp3[substr(temp3, 1, 1) %in% c("3", "5", "6", "7", "8", "9", "15", "17")]
    }
  }
  series %<>% select(goal = goal2, target = target2, indicator = indicator2, release:uri)
  save(series, file = file.path(datadir, "series.Rdata"))
  
  # Each series has a different set of attributes, first compile all possible attributes
  for (i in 1:nrow(series)) {
    r <- POST("https://unstats.un.org/sdgapi/v1/sdg/Series/DataCSV",
              body = paste0("seriesCodes=", series[i, "code"], "&areaCodes=1&timePeriodStart=2000&timePeriodEnd=2010"),
              httr::add_headers(`Content-Type` = 'application/x-www-form-urlencoded',
                                `Accept` = 'application/octet-stream')) 
    temp <- read.table(text = rawToChar(r$content), quote = "\"'", sep = ",", header = T)
    if(i == 1) {
      varnames <- colnames(temp)
    } else {
      varnames <- c(varnames, colnames(temp))
    }
  }
  varnames <- unique(varnames)
  
  # Download and save all series
  for (i in 1:nrow(series)) {
    r <- POST("https://unstats.un.org/sdgapi/v1/sdg/Series/DataCSV",
              body = paste0("seriesCodes=", series[i, "code"], "&timePeriodStart=2000"),
              httr::add_headers(`Content-Type` = 'application/x-www-form-urlencoded',
                                `Accept` = 'application/octet-stream')) 
    temp <- read.table(text = rawToChar(r$content), quote = "\"'", sep = ",", header = T)
    for (j in varnames) {
      if (!(j %in% colnames(temp))) {temp[, j] <- NA}
    }
    if(i == 1) {
      data <- temp
    } else {
      temp <- temp[, varnames]
      data <- rbind(data, temp)
    }
  }
  
  # Select only the desired data series
  sdg_indicators <- c("SH_DYN_MORT", #3.2.1: Under-five mortality rate, by sex (deaths per 1,000 live births)
                      "SH_ACS_UNHC", #3.8.1: Universal health coverage (UHC) service coverage index
                      "SG_GEN_PARL", #5.5.1: Proportion of seats held by women in national parliaments (% of total number of seats)
                      "SH_H2O_SAFE", #6.1.1: Proportion of population using safely managed drinking water services, by urban/rural (%)
                      "ER_H2O_WUEYST", #6.4.1: Water Use Efficiency (United States dollars per cubic meter)
                      "EG_EGY_PRIM", #7.3.1: Energy intensity level of primary energy (megajoules per constant 2017 purchasing power parity GDP)
                      "FB_BNK_ACCSS", #8.10.2: Proportion of adults (15 years and older) with an account at a financial institution or mobile-money-service provider, by sex (% of adults aged 15 years and older)
                      "EN_ATM_CO2GDP", #9.4.1: Carbon dioxide emissions per unit of GDP PPP (kilogrammes of CO2 per constant 2017 United States dollars)
                      "ER_PTD_TERR", #15.1.2: Terrestrial biodiversity area as % total protected areas
                      "IT_NET_BBND" #17.6.1: Fixed broadband subscriptions per 100 inhabitants, by speed (per 100 inhabitants)
                      )
  sdgdf <- data %>% filter(SeriesCode %in% sdg_indicators) %>%
    mutate_at(vars(GeoAreaCode), function(x) str_pad(x, 3, side = "left", pad = "0"))
  write_csv(sdgdf, file = file.path(datadir, dat, "SDG_data_raw.csv"))
  

# Source: IEA -------------------------------------------------------------

  ### Generally on IEA data -------------------------------------------------
  # Most of these come originally from IEA but have been downloaded from World Bank.
  # They have a WB indicator code but are calculated or directly based on IEA
  # data. Several WB versions are discontinued and only have data up until 2014 or
  # 2015. Much data originally comes from The IEA World Energy Statistics and
  # Balances can be found in the OECD iLibrary
  
  # Description of product and flow can be found in
  # http://wds.iea.org/wds/pdf/WORLDBAL_Documentation.pdf
  
  
  # UNCTAD staff have access to IEA data via OECD iLibrary. Authentication outside
  # Palais via:
  # https://go.openathens.net/redirector/un.org?url=https://www.oecd-ilibrary.org/
  
  # Direct link to IEA World Energy Statistics and Balances is
  # https://doi.org/10.1787/enestats-data-en
  #
  # In OECD iLibrary the database is split up into several datasets.
  #
  # Description of product and flow can be found in
  # http://wds.iea.org/wds/pdf/WORLDBAL_Documentation.pdf
  #
  
  #### WIND - World Indicators -------------------------------------------------
  
  # Several indicators are directly available though the dataset *World
  # Indicators* Flow can be selected in the **Customize** menu. The following are
  # of interest:
  #
  # - **Electricity consumption/population (kWh per capita)** 
  #   FLOW: **ELEPOP**
  
  # Data downloaded in a csv format, available in "Downloaded"
  
  iea_data <- read_csv(file.path(downdir, "WIND_2024_downloaded.csv"))
  ieadf <- iea_data %>%
    filter(ENERGY_INDICATOR=="ELECONS_POP",
           TIME_PERIOD %in% c(FirstYear:LastYear))
  write_csv(ieadf, file = file.path(datadir, dat, "IEA_data_raw.csv"))
  

# Source: World Marriage Data ---------------------------------------------

  # UN Population Division: World Marriage Data 2019
  # Data downloaded directly from https://population.un.org/MarriageData/Index.html#/home
  # Data file includes a sheet on singulate mean age at marriage (SMAM)
  
  # PK20241104: NOT RUN, no update available, take the clean version of data from last year

#url <-  "https://population.un.org/MarriageData/documents/UNPD_WMD_2019_MARITAL_STATUS.xlsx"
#GET(url, write_disk(tf <- tempfile(fileext = ".xlsx")))
#mardf <- read_excel(tf, skip = 2, sheet="SMAM")

#write_csv(mardf, file = file.path(datadir, dat, "Marriage_data_raw.csv"))


# Source: EDGAR CO2 emissions ---------------------------------------------

  # CO2 emissions of all world countries, 2024 Report
  # All emissions, except for CO2 emissions from fuel combustion, are from the EDGAR 
  # (Emissions Database for Global Atmospheric Research) Community GHG database comprising 
  # IEA-EDGAR CO2, EDGAR CH4, EDGAR N2O and EDGAR F-gases version EDGAR_2024_GHG (2024).
  
  # IEA-EDGAR CO2 (v3) data are based on data from IEA (2023) Greenhouse Gas Emissions 
  # from Energy, www.iea.org/statistics, as modified by the Joint Research Centre”, 
  # licensed under CC BY-NC-ND 4.0. Users of IEA-EDGAR CO2 data should contact 
  # the IEA at compliance@iea.org for permission to use.

  # PK20242204: ??? Data to complement official CO2 emissions intensity
  # ??? Maybe it is easier to use this source only? TBD
  # GHG emissions include CO2 (fossil only), CH4, N2O and F-gases. 
  # They are aggregated using Global Warming Potential values from IPCC AR5 (GWP-100 AR5).
  # values in fossil_CO2_per_GDP_by_country sheet are expressed in t CO2/kUSD/yr

url <-  "https://edgar.jrc.ec.europa.eu/booklet/EDGAR_2024_GHG_booklet_2024_fossilCO2only.xlsx"
GET(url, write_disk(tf <- tempfile(fileext = ".xlsx")))
edgardf <- read_excel(tf, sheet="fossil_CO2_per_GDP_by_country")

write_csv(edgardf, file = file.path(datadir, dat, "EDGAR_CO2_data_raw.csv"))


# Source: U.S. Energy Information Administration --------------------------

# Data on electricity consumption from: https://www.eia.gov/
# Link: https://www.eia.gov/international/data/world/electricity/electricity-consumption?pd=2&p=0000002&u=0&f=A&v=mapbubble&a=-&i=none&vo=value&t=C&g=00000000000000000000000000000000000000000000000001&l=249-ruvvvvvfvtvnvv1vrvvvvfvvvvvvfvvvou20evvvvvvvvvvnvvvs0008&s=315532800000&e=1672531200000&
# Data downloaded, no API found

eiadf <- read_csv(file.path(downdir, "EIA_electricity_20241104.csv"), skip = 1) %>%
  slice(-1) %>%
  rename(ISO3=API) %>%
  mutate(ISO3=sub("^.*-([A-Z]+)-.*$", "\\1", ISO3))

write_csv(eiadf, file = file.path(datadir, dat, "US_EIA_data_raw.csv"))

# Source: World Income Inequality Database --------------------------------

url <- "https://www.wider.unu.edu/sites/default/files/Data/WIID_28NOV2023.xlsx"
GET(url, write_disk(tf <- tempfile(fileext = ".xlsx")))
wiiddf <- read_excel(tf)

write_csv(wiiddf, file = file.path(datadir, dat, "WIID_data_raw.csv"))


# Source: World Population Prospects --------------------------------------

# World Population Prospects
  # Data downloaded directly from https://population.un.org/wpp/Download/Standard/MostUsed/
  # Data file includes estimates from 1950 with projected time series until 2100
url <- "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/EXCEL_FILES/1_General/WPP2024_GEN_F01_DEMOGRAPHIC_INDICATORS_COMPACT.xlsx"
GET(url, write_disk(tf <- tempfile(fileext = ".xlsx")))
dfest <- read_excel(tf, skip = 16, sheet="Estimates")
colnames(dfest)
dfmed <- read_excel(tf, skip = 16, sheet="Medium variant")

cols <- c("Index",
          "Variant",                                                                                       
          "Region, subregion, country or area *",                                                         
          "Notes",                                                                                        
          "Location code",                                                                               
          "ISO3 Alpha-code",                                                                               
          "ISO2 Alpha-code",                                                                               
          "SDMX code**",                                                                                  
          "Type",                                                                                         
          "Parent code",                                                                                  
          "Year",                                                                                         
          #"Total Population, as of 1 January (thousands)",                                              
          "Total Population, as of 1 July (thousands)",                                                   
          "Male Population, as of 1 July (thousands)",                                                   
          "Female Population, as of 1 July (thousands)")

popest <- dfest %>%
  select(all_of(cols)) %>%
  rename(region='Region, subregion, country or area *',
         m49='Location code',
         iso3='ISO3 Alpha-code',
         iso2='ISO2 Alpha-code',
         sdmx='SDMX code**',
         pop_tot='Total Population, as of 1 July (thousands)',
         pop_m='Male Population, as of 1 July (thousands)',
         pop_f='Female Population, as of 1 July (thousands)') %>%
  filter(Type %in% "Country/Area") %>%
  mutate_at(vars(pop_tot, pop_m, pop_f), as.numeric) %>%
  select(Variant, region, m49, Year, pop_tot, pop_m, pop_f)

popmed <- dfmed %>%
  select(all_of(cols)) %>%
  rename(region='Region, subregion, country or area *',
         m49='Location code',
         iso3='ISO3 Alpha-code',
         iso2='ISO2 Alpha-code',
         sdmx='SDMX code**',
         pop_tot='Total Population, as of 1 July (thousands)',
         pop_m='Male Population, as of 1 July (thousands)',
         pop_f='Female Population, as of 1 July (thousands)') %>%
  filter(Type %in% "Country/Area") %>%
  mutate_at(vars(pop_tot, pop_m, pop_f), as.numeric) %>%
  select(Variant, region, m49, Year, pop_tot, pop_m, pop_f)

popdf <- popest %>%
  bind_rows(popmed) %>%
  mutate_at(vars(m49), function(x) str_pad(x, 3, side = "left", pad = "0")) %>%
  filter(Year %in% c(FirstYear:LastYear)) %>%
  arrange(m49, Year)

write_csv(popdf, file = file.path(datadir, dat, "WPP_data_raw.csv"))


# Source: UNSD NAMAD ------------------------------------------------------
# GDP data: GDP, at constant 2015 prices - US Dollars
# GDP_USD2015
# https://unstats.un.org/unsd/snaama/Basic
# It needs to be chosen from the drop down select field, and then 
## click [Send request], and then 
## click [Export to csv], (downloaded file is called Results.csv) and then 
## rename the downloaded file
## TODO Move to https://unstats.un.org/unsd/snaama/downloads
url <- "https://unstats.un.org/unsd/amaapi/api/file/6"
GET(url, write_disk(tf <- tempfile(fileext = ".xlsx")))
dfgdp <- read_excel(tf, skip = 2)

### Tanzania
# M49 is 834
# Mainland 835, Zanzibar 836 -> group into 834
### Sudan corresponds to M49 codes

gdpdf <- dfgdp %>%
  mutate_at(vars(CountryID), function(x) str_pad(x, 3, side = "left", pad = "0")) %>%
  pivot_longer(-c(CountryID, Country, IndicatorName), names_to = "Year", values_to = "Value") %>%
  filter(IndicatorName=="Gross Domestic Product (GDP)",
         Year %in% c(FirstYear:LastYear)) %>%
  mutate(CountryID=recode(CountryID,
                          "835"="834",
                          "836"="834")) %>%
  group_by(CountryID, Year, IndicatorName) %>%
  summarise_at(vars(Value), sum, na.rm=TRUE) %>%
  ungroup()

write_csv(gdpdf, file = file.path(datadir, dat, "GDP_data_raw.csv"))


# Source: UNCTAD data centre ----------------------------------------------
# Synchronous request
# UNCTADstat report: Goods and services (BPM6): Trade openness indicators, annual
# Version: 30 Aug. 2024
# Source: 
# https://unctadstat.unctad.org/datacentre/dataviewer/US.GoodsAndServTradeOpennessBpm6

# Parameters
# Input file
#Define the path of the csv file that stores the data returned by the API
tempFilePath <- "synchrone.csv.gz"

#Define the data center user info
ClientId <- "2135e1ec-8508-4879-a9cd-a915f43ac676"
ClientSecret <- "GCqkJAbtgIqZmtXRPkD9Lvf58e+hXjmpfBKGySadU4o="

#===============================================================================

# Series 2106 - Total trade in goods and services
# Flow 21 - Sum of imports and exports (20 - Average of imports and exports)
# Measure 5025 - % of GDP
filter <- "Flow/Code eq '21' and Series/Code eq '2106'"

#Download data as csv and store it as a file
curlHandle <- curl::new_handle() |>
  curl::handle_setform(
    "$select"="Economy/Label, Economy/Code, Year, Value",
    "$filter"=filter,
    "$orderby"="Economy/Order asc ,Year asc",
    "$compute"="round(M5025/Value, 3) as Value",
    #"$compute"="round(M0100/Value div 100000, 0) as US_at_current_prices_in_millions_Value",
    #"$compute"="round(M0100/Value div 1000000, 0) as US_at_current_prices_in_millions_Value, M0100/Footnote/Text as US_at_current_prices_Footnote, M0100/MissingValue/Label as US_at_current_prices_MissingValue",
    "$format"="csv",
    "compress"="gz"
  ) |>
  curl::handle_setheaders(
    "ClientId"=ClientId,
    "ClientSecret"=ClientSecret)

curl::curl_download("https://unctadstat-user-api.unctad.org/US.GoodsAndServTradeOpennessBpm6/cur/Facts?culture=en", tempFilePath, handle = curlHandle)

#Load downloaded data in a dataframe
data <- utils::read.csv(
  gzfile(tempFilePath),
  header = TRUE,
  na.strings = "",
  encoding = "UTF-8",
  colClasses = c("character","character","integer","double")
)

#as_tibble(unctaddf)

write_csv(unctaddf, file = file.path(datadir, dat, "UNCTAD_data_raw.csv"))

# Source: UNESCO UIS ----------------------------------------------
# Education data could be downloaded directly from UNESCO website
# World Bank is a secondary source and not preferable
# Some data missing in WDI
# Buld download available here: https://uis.unesco.org/bdds
# File: Other Policy Relevant Indicators (OPRI) (last update: September 2024)
# Files stored in Downloaded/UNESCO
dfunesco <- read_csv(file.path(downdir, "UNESCO/OPRI_DATA_NATIONAL.csv"))
unescodf <- dfunesco %>%
  filter(indicator_id %in% c("GER.2T3", "GER.2T3.GPIA"),
         year %in% c(FirstYear:LastYear))

write_csv(unescodf, file = file.path(datadir, dat, "UNESCO_data_raw.csv"))


# Source: World Bank Poverty and Inequality Platform ----------------------
# Poverty headcount ratio at $3.65 a day (2017 PPP) (% of population)
# Data downloaded from https://pip.worldbank.org/poverty-calculator
dfpip <- read_csv(file.path(downdir, "pip_2024.csv"))
pipdf <- dfpip %>%
  filter(reporting_year %in% c(FirstYear:LastYear))

write_csv(pipdf, file = file.path(datadir, dat, "WBPIP_data_raw.csv"))

# }



