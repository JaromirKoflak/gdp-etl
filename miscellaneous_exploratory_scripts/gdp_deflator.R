

library(tidyverse)
library(readxl)


last_year = 2024

# Directories
datadir = file.path(getwd(), "data")
outputdir = file.path(getwd(), "output")

dfgdp = read_csv(file.path(outputdir, "gdp_update.csv"), show_col_types=F) 


all_subseries <- read_csv("https://usis.unctad.unctad.org/UsisDWDataService/Subseries?$format=csv")
all_subseries %>% 
  filter(str_detect(Series_Label, "Exchange"),
         Frequency_Code == "A",
         str_detect(Source_Label, "UN")) %>% 
  select(Series_Label, Series_Code, Source_Code, Measure_Code)

read_usis <- function(series, source, measure) {
  paste0(
    "https://usis.unctad.unctad.org/UsisDWDataService/",
    "Series", series, "Source", source, "Measure", measure,
    "FrequencyA/GetLastVersion()/Data?$format=csv"
  ) %>% 
    read_csv(show_col_types = F) %>% 
    return 
}

gdp_current <- read_usis("5100", "4805", "0100")

gdp_constant <- read_usis("5100", "4805", "0940")

gdp_deflators <- read_usis("5105", "0101", "6700") 

cpi <- read_usis("5301", "0101", "6510")
cpi %>% 
  filter(Value == 100)

cpi %>% 
  filter(str_detect(Country_Label, "Maarten")) %>% 
  select(Year, Country_Label, Value) 
cpi %>% 
  filter(str_detect(Country_Label, "Curacao")) %>% 
  select(Year, Country_Label, Value)

gdp_deflators %>% 
  filter(Value == 100)

gdp_deflators %>% 
  filter(str_detect(Country_Label, "aa")) %>% 
  distinct(Country_Label)

exchange_rates <- read_usis("5201", "0101", "4001")
# exchange_rates <- read_usis("5201", "0115", "0200") # Not working
# exchange_rates <- read_usis("5201", "4805", "0200") # No data for 2024
# exchange_rates <- read_usis("5205", "0101", "0200") # Entire crosstable, same values
# exchange_rates <- read_usis("5201", "4805", "4000") # Not working
# exchange_rates <- read_usis("5201", "0101", "4000") # Not working

ppp1 <- read_usis("5212", "0101", "4000")
ppp2 <- read_usis("5210", "2304", "4000")
ppp3 <- read_usis("5210", "3102", "4000")
ppp4 <- read_usis("5210", "5103", "4000")
ppp1 %>% 
  distinct(Year) %>% 
  arrange(desc(Year))

deflator_USD <- gdp_deflators %>%
  left_join(
    exchange_rates %>%
      select(Year, Country_Code, Value),
    by = join_by(Country_Code, Year),
    suffix = c("", ".exg")
  ) %>%
  select(Country_Code, Country_Label, Year, Value, Value.exg) %>%
  arrange(Country_Code, Year) %>%
  mutate(Deflator_exg = Value / Value.exg) %>%
  group_by(Country_Label) %>%
  mutate(Deflator2015 = ifelse(length(Deflator_exg[Year==2015]) == 1,
                               Deflator_exg[Year==2015],
                               NA)
  ) %>%
  ungroup %>%
  mutate(Deflator_USD = 100 * Deflator_exg / Deflator2015)

# deflator_USD <- gdp_deflators %>% 
#   left_join(
#     exchange_rates_list[[4]] %>% 
#       filter(Economy_Code == "842") %>% 
#       select(Year, ForeignEconomy_Code, Value),
#     by = join_by(Country_Code == ForeignEconomy_Code
#                  ,Year == Year),
#     suffix = c("", ".exg")
#   ) %>% 
#   select(Country_Code, Country_Label, Year, Value, Value.exg) %>% 
#   arrange(Country_Code, Year) %>% 
#   mutate(Deflator_exg = Value / Value.exg) %>% 
#   group_by(Country_Label) %>% 
#   mutate(Deflator2015 = ifelse(length(Deflator_exg[Year==2015]) == 1,
#                                Deflator_exg[Year==2015],
#                                NA)
#   ) %>% 
#   ungroup %>% 
#   mutate(Deflator_USD = 100 * Deflator_exg / Deflator2015)

deflator_USD %>% 
  filter(Year == 2024)

estimate_last_year = function(df) {
  estimate_constant = df %>%
    filter(Year == last_year-1,
           Variable == "GDP_at_constant_prices_2015") %>%
    left_join(
      read_excel(file.path(datadir, "GDP growth rates.xlsx")) %>%
        select(UNCTcc, last_col()),
      by = join_by(Economy_Code == UNCTcc)
    ) %>%
    mutate(
      Year = last_year,
      Value = Value * (1+across(last_col())[[1]]/100)
    ) %>%
    select(!last_col())
  
  estimate_current = estimate_constant %>%
    filter(Year == last_year, 
           Variable == "GDP_at_constant_prices_2015") %>% 
    left_join(
      deflator_USD %>% 
        select(Country_Code, Year, Deflator_USD),
      by = join_by(Economy_Code == Country_Code,
                   Year == Year)
    ) %>% 
    mutate(Variable = "GDP_at_current_prices",
           Value = Value * 100 / Deflator_USD) %>% 
    select(!Deflator_USD)
  
  # return(df %>% bind_rows(estimate_constant, estimate_current))
  
  return(df %>%
           filter(Year != last_year) %>% 
           bind_rows(estimate_constant, estimate_current) %>% 
           arrange(Economy_Code, Year))
}

dfgdp_estimates = estimate_last_year(dfgdp) %>% 
  filter(Year == last_year) %>% 
  arrange(Economy_Code)

# 2 more missing values for 2024 -> Do not use
# gdp_deflators2 <- read_csv(
#   paste0("https://usis.unctad.unctad.org/UsisDWDataService/",
#          "Series", 5105, "Source", "2304", "Measure", 6700,
#          "FrequencyA/GetLastVersion()/Data?$format=csv"
#   ), 
#   show_col_types = F)


# Data until year 2017 -> Do not use
# gdp_deflators3 <- read_csv(
#   paste0("https://usis.unctad.unctad.org/UsisDWDataService/",
#          "Series", 5105, "Source", "0101", "Measure", 6473,
#          "FrequencyA/GetLastVersion()/Data?$format=csv"
#   ), 
#   show_col_types = F)

df_deflator = dfgdp %>% 
  pivot_wider(
    names_from = "Variable", 
    values_from = "Value") %>% 
  left_join(
    deflator_USD %>% 
      # group_by(Country_Code) %>% 
      transmute(
        Year = Year,
        Country_Code = Country_Code,
        Deflator_DSIB = Deflator_USD
      ),
    by = join_by(Economy_Code == Country_Code,
                 Year == Year)
  ) %>% 
  mutate(Deflator_UNSD = 100 * GDP_at_current_prices  / GDP_at_constant_prices_2015) %>%
  mutate(Deflator_UNSD = replace(
                           Deflator_UNSD,
                           Year == 2024,
                           NA),
         Diff = Deflator_DSIB - Deflator_UNSD) %>% 
  select(Economy_Code, Economy_Label, Year, Deflator_DSIB, Deflator_UNSD, Diff) 

df_deflator %>%   
  filter(!near(Deflator_DSIB, Deflator_UNSD, tol=1))  %>%
  group_by(Economy_Label) %>% 
  filter(Diff %in% c(max(abs(Diff)), -max(abs(Diff)))) %>% 
  arrange(desc(abs(Diff))) 

df_deflator %>% 
  filter(Year == 2023) %>% 
  arrange(desc(abs(Diff))) 

df_deflator %>% 
  filter(Year == 2023) %>% 
  ggplot(aes(Diff)) +
  geom_histogram() +
  scale_x_log10() +
  theme_bw()

df_deflator %>% 
  filter(Economy_Label == "Czechia") %>% 
  tail(10)

df_deflator %>% 
  filter(Year == 2024,
         is.na(Deflator_DSIB),
         str_length(Economy_Code) <= 3) %>% 
  left_join(
    gdp_deflators %>% 
      select(Country_Code, Year, Value),
    join_by(Economy_Code == Country_Code, 
            Year == Year)
  ) %>% 
  pull(Economy_Label) %>% 
  paste(collapse = ", ")
  

df_deflator %>% 
  filter(Economy_Label == "Zimbabwe") %>% 
  tail(10)

gdp_deflators %>% 
  filter(Year == 2024) %>% 
  distinct(Country_Label) %>% 
  arrange(Country_Label) %>% 
  pull()



get_gdp_cpi = function() {
  
  cpi <- read_usis("5301", "0101", "6510")
  
  cpi %>% 
    select(Country_Code, Country_Label, Year, Value) %>% 
    arrange(Country_Code, Year) %>% 
    group_by(Country_Label) %>% 
    mutate(Value2015 = ifelse(length(Value[Year==2015]) == 1, # For each economy get CPI for the year 2015 
                                 Value[Year==2015],
                                 NA)
    ) %>% 
    ungroup %>% 
    mutate(CPI = 100 * Value / Value2015) %>% # CPI rebased to 2015
    return
}
