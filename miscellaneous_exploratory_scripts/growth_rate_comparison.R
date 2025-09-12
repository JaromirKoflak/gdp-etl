
################################
## COMPARE GROWTH RATE TABLES ##
################################

library(dplyr)
library(tidyr)
library(readxl)


### Getting the path of your current open file
current_path = rstudioapi::getActiveDocumentContext()$path 
setwd(dirname(current_path))
print(getwd())

datadir = paste(getwd(), "/data", sep = "")
outputdir = paste(getwd(), "/output", sep = "")

labels = read.csv(file.path(datadir, "lab_all.csv")) %>% 
  mutate(Code = as.numeric(Code)) %>% 
  mutate(Label = replace(Label, Code == 498, "Republic of Moldova")) %>% 
  mutate(Label = replace(Label, Code == 410, "Republic of Korea")) %>% 
  mutate(Label = replace(Label, Code == 890, "Yugoslavia, Soc. Fed. Rep. of"))

growth_rate_excel = read_excel(file.path(datadir, "GDP growth rates.xlsx")) %>%
  pivot_longer(-c(1:2), names_to = "Year", values_to = "Excel") %>% 
  mutate(UNCTcc = as.numeric(UNCTcc), Year = as.numeric(Year))  

growth_rate_unctadstat = read.csv(file.path(datadir, "US.GDPGR_20250716_065711.csv")) %>% 
  left_join(labels, by = join_by(Economy_Label == Label)) %>% 
  mutate(Unctadstat = Annual_average_growth_rate_Value)


# Check that all economies have been assigned their code
# Should return 0 rows
growth_rate_unctadstat %>% 
  filter(is.na(Code)) %>% 
  distinct(Economy_Label)

growth_rate = 
  inner_join(
    growth_rate_unctadstat, 
    growth_rate_excel, 
    by = join_by(Code == UNCTcc, 
                 Period_Label == Year)) %>% 
  mutate(Year = Period_Label) %>% 
  select(Code, Economy_Label, Year, Excel, Unctadstat) %>% 
  mutate(diff = Excel - Unctadstat) %>% 
  arrange(desc(abs(diff)))

growth_rate %>% 
  filter(Code == 834) %>% head(20)
growth_rate %>% 
  filter(Code == 834, Year == 2023)
growth_rate %>% 
  filter(Year == 2024) %>% 
  head(20)
