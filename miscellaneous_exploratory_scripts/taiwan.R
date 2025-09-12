
library(tidyverse)
library(plotly)
library(readxl)

### Getting the path of your current open file
current_path = rstudioapi::getActiveDocumentContext()$path
setwd(dirname(current_path))
print(getwd())

# Directories
datadir = paste(getwd(), "/data", sep = "")
outputdir = paste(getwd(), "/output", sep = "")

# Taiwan National Statistics
# https://eng.stat.gov.tw/cl.aspx?n=4015
twstat = read_csv(file.path(datadir, "taiwan_national_statistics.csv"), skip = 2) %>% 
  na.omit() %>% 
  transmute(
    Year = Period,
    GDP_at_current_prices = `GDP (Million U.S.$,at Current Prices)` * 1e6,
    GDPPC_at_current_prices = `Per Capita GDP ( U.S.$,at Current Prices )`,
    Source = "TWNS") %>% 
  pivot_longer(c(GDP_at_current_prices, GDPPC_at_current_prices), names_to = "Variable", values_to = "Value") %>% 
  mutate(Year = as.numeric(Year))
twstat


# UNIDO
# https://stat.unido.org/data/download?dataset=national-accounts&country=158 
unidostat = read_csv(file.path(datadir, "unido_taiwan.csv")) %>% 
  filter(Variable %in% c("GDP (Gross Domestic Product), constant 2015 USD", "GDP (Gross Domestic Product), current USD")) %>% 
  transmute(
    Year = Year,
    Source = "UNIDO",
    Variable = recode(Variable,
                     `GDP (Gross Domestic Product), constant 2015 USD` = "GDP_at_constant_prices",
                     `GDP (Gross Domestic Product), current USD` =  "GDP_at_current_prices"),
    Value = Value)   
unidostat


# UNCTAD 
unctadstat = read_csv(file.path(datadir, "US.GDPTotal_20250718_104458.csv")) %>% 
  filter(Economy_Label == "China, Taiwan Province of") %>% 
  transmute(
    Year = Year, 
    GDP_at_constant_prices = US_at_constant_prices_2015_Value, 
    GDP_at_current_prices = US_at_current_prices_Value,
    Source = "UNCTAD") %>% 
  pivot_longer(c(GDP_at_constant_prices, GDP_at_current_prices), names_to = "Variable", values_to = "Value")
unctadstat


# IMF
# https://www.imf.org/external/datamapper/NGDPD@WEO/TWN?zoom=TWN&highlight=TWN
imfdata = read_excel(file.path(datadir, "imf-dm-export-20250807.xls")) %>% 
  na.omit() %>% 
  pivot_longer(-1) %>% 
  transmute(
    Year = as.numeric(name),
    Source = "IMF",
    Variable = "GDP_at_current_prices",
    Value = value*1e9
  )
imfdata

### Taiwan NSO, UNIT: Million NT$
# Fernando's Code
last_year = 2023
t_1951 <- (1951 - 1951) * 100 + 4000
t_1980 <- (1980 - 1951) * 100 + 4000
t_1981 <- (1981 - 1951) * 100 + 4000
t_last <- (last_year - 1951) * 100 + 4000

# 1.1 Principal Figures 
# Used for Exchange rates
url <- paste0("https://nstatdb.dgbas.gov.tw/dgbasall/webMain.aspx?sys=220&funid=E018101010&outmode=3&cycle=4&outkind=3&compmode=0&ratenm=Value&fldlst=111111111111111&compmode=0",
              "&ymf=", t_1951, "&ymt=", t_last, "&rdm=R164860&eng=1")
temp <- read_csv(url, skip = 2, show_col_types = F)
data1 <- temp %>% slice(-c((nrow(.)-3):nrow(.)))

# 2.1 Expenditures on GDP Annual & Quarterly (1951-1980)
# Using only Annual data
url <- paste0("https://nstatdb.dgbas.gov.tw/dgbasall/webMain.aspx?sys=220&funid=E018102010&outmode=3&cycle=4&outkind=3&compmode=0&ratenm=Value&fldlst=111&codlst0=100000000000011001001001111&compmode=0",
        "&ymf=", t_1951, "&ymt=", t_1980, "&rdm=R63170&eng=1") 
temp <- read_csv(url, skip = 2, show_col_types = T, na="--", 
                 col_types = cols(
                   `At Current Prices` = col_double(),
                   `Chained (2021) Dollars` = col_double()))
data2 <- temp %>% slice(-c((nrow(.)-5):nrow(.)))

# 2.2 Expenditures on GDP Annual (since 1981)
url <- paste0("https://nstatdb.dgbas.gov.tw/dgbasall/webMain.aspx?sys=220&funid=E018102050&outmode=3&cycle=4&outkind=3&compmode=0&ratenm=Value&fldlst=111&codlst0=111001001001111111111&compmode=0",
        "&ymf=", t_1981, "&ymt=", t_last, "&rdm=R23908&eng=1")
temp <- read_csv(url, skip = 2, show_col_types = T, na="--", 
                 col_types = cols(
                   `At Current Prices` = col_double(),
                   `Chained (2021) Dollars` = col_double()))
data3 <- temp %>% slice(-c((nrow(.)-3):nrow(.)))

rebase_factor = data3 %>%
  filter(Period == 2015, Expenditure == "8. GDP") %>%
  summarise(rebase_factor = `At Current Prices` / `Chained (2021) Dollars`) %>%
  pull %>%
  print

nsodata = data2 %>%
  add_row(data3) %>% 
  filter(Expenditure %in% c("6.GDP", "8. GDP")) %>% 
  left_join(data1) %>% 
  mutate(Exchange_rate = `GDP (Million N.T.$,at Current Prices)` / `GDP (Million U.S.$,at Current Prices)`) %>% 
  transmute(
    Year = Period,
    Source = "NSO",
    # Constant prices are calculated using the exchange rate of the base year for all years  
    GDP_at_constant_prices = `Chained (2021) Dollars` * 1e6 / Exchange_rate[Period==2015] * rebase_factor,
    # Current prices are calculated using the exchange rate of that year  
    GDP_at_current_prices = `At Current Prices` * 1e6 / Exchange_rate,
  ) %>%
  pivot_longer(-c(1:2), names_to = "Variable", values_to = "Value") %>%
  mutate(Year = as.numeric(Year)) %>%
  print


# Everything combined
taiwandf = twstat %>% 
  add_row(unidostat) %>% 
  add_row(unctadstat) %>% 
  add_row(imfdata) %>% 
  add_row(nsodata)
  
p = taiwandf %>% 
  filter(Variable == "GDP_at_constant_prices") %>%
  filter(Source %in% c("UNIDO", "UNCTAD", "NSO")) %>%
  ggplot(aes(x=Year, y=Value, color=Source, linetype=Variable)) +
    geom_line() + 
    theme_bw() 
p
ggplotly(p)

p = taiwandf %>% 
  filter(Variable == "GDP_at_current_prices") %>%
  filter(Source %in% c("UNIDO", "UNCTAD", "NSO")) %>%
  ggplot(aes(x=Year, y=Value, color=Source, linetype=Variable)) +
  geom_line() + 
  theme_bw() 
p
ggplotly(p)


