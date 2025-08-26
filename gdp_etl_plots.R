
rm(list = ls())

library(tidyverse)
library(readxl)
library(httr)
library(gridExtra)
library(plotly)

##############
## SETTINGS ##
##############

### Getting the path of your current open file
current_path = rstudioapi::getActiveDocumentContext()$path
setwd(dirname(current_path))
print(getwd())

# Directories
datadir = file.path(getwd(), "data")
outputdir = file.path(getwd(), "output")

# Valid year ranges
dim_countries = read.csv(file.path(datadir, "Dim_countries.csv")) %>% 
  filter(IsTarget == "True")

# Groups of economies (World, G20, Europe...)
economy_groups = read.csv(file.path(datadir, "Dim_Countries_Hierarchy_All.csv")) %>% 
  mutate(Parent_Code = as.character(Parent_Code))

# Economy labels
labels = read.csv(file.path(datadir, "lab_all.csv")) 

# Updated data 
dfgdp = read_csv(file.path(outputdir, "gdp_update.csv")) 
dfgdp 
dfgdp %>% filter(Economy_Code == "0000")
dfgdp %>% filter(Economy_Code == "158")

###########################
## YEAR RANGE COMPARISON ##
###########################

get_unsd_gdp_data = function() {
  # API calls
  url1 <- "https://unstats.un.org/unsd/amaapi/api/file/6"
  GET(url1, write_disk(tf <- tempfile(fileext = ".xlsx")))
  dfgdp_constant <- read_excel(tf, skip = 2) %>% 
    filter(IndicatorName == "Gross Domestic Product (GDP)") %>%
    pivot_longer(4:57, names_to = "Year", values_to = "GDP_at_constant_prices_2015")
  
  url2 <- "https://unstats.un.org/unsd/amaapi/api/file/2"
  GET(url2, write_disk(tf <- tempfile(fileext = ".xlsx")))
  dfgdp_current <- read_excel(tf, skip = 2) %>% 
    filter(IndicatorName == "Gross Domestic Product (GDP)") %>% 
    pivot_longer(4:57, names_to = "Year", values_to = "GDP_at_current_prices")
  
  df = 
    inner_join(
      dfgdp_constant, 
      dfgdp_current,
      by = c("CountryID", "Year")
    ) %>% 
    pivot_longer(
      c(5,8), 
      names_to = "Variable", 
      values_to = "Value"
    ) %>% 
    mutate(
      Economy_Code = str_pad(CountryID, 3, side = "left", pad = "0"),
      Year = as.numeric(Year)
    ) %>% 
    select(
      Economy_Code, Year, Variable, Value
    )
  
  return(df)
}

add_economy_labels = function(df) {
  return(
    df %>% 
      left_join(
        labels,
        by = join_by(Economy_Code == Code)
      ) %>% 
      rename(Economy_Label = Label) %>% 
      select(Economy_Code, Economy_Label, everything())
  )
}

# Shows valid year range and year range of the data
compare_years = function(country_name) {
  print("Requested year range")
  dim_countries %>%
    filter(str_detect(Label, country_name)) %>%
    print
  print("Input data year range")
  input_data %>%
    filter(str_detect(Economy_Label, country_name)) %>%
    filter(!is.na(Value)) %>%
    group_by(Economy_Code) %>%
    summarise(Economy_Label[1], min(Year), max(Year)) %>%
    print
}

input_data = get_unsd_gdp_data() %>% 
  add_economy_labels()

input_data %>%
  filter(Economy_Label == "Kosovo",
         !is.na(Value))

compare_years(".*Kos.*")
compare_years(".*Ser.*")
compare_years(".*Mon.*")

######################
## PROBLEMATIC DATA ##
######################

# Missing valid range
# -> Add together to create United Republic of Tanzania
input_data %>%
  left_join(
    dim_countries %>% select(!c(Label, IsTarget)),
    by=join_by(Economy_Code == Code)
  ) %>%
  filter(is.na(ValidTo)) %>%
  distinct(Economy_Code, Economy_Label)

# Missing all GDP values for valid ranges
# -> Does not matter
input_data %>%
  right_join(
    dim_countries %>% select(!c(IsTarget)),
    by=join_by(Economy_Code == Code)
  ) %>%
  filter(is.na(Economy_Label)) %>%
  print(n=46)

# Values outside the valid range
# -> Delete
input_data %>%
  left_join(
    dim_countries %>% select(!c(Label, IsTarget)),
    by=join_by(Economy_Code == Code)
  ) %>%
  filter(Year < ValidFrom | Year > ValidTo) %>%
  filter(!is.na(Value)) %>% 
  group_by(Economy_Code, Economy_Label) %>% 
  summarise(from = min(Year), to = max(Year)) %>%
  print(n=42)

# Missing GDP values inside the valid range
# -> Add values for the member states together and check if plausible
input_data %>%
  left_join(
    dim_countries %>% select(!c(Label, IsTarget)),
    by=join_by(Economy_Code == Code)
  ) %>%
  filter(Year >= ValidFrom, Year <= ValidTo) %>%
  filter(is.na(Value)) %>%
  group_by(Economy_Label) %>%
  summarise(from = min(Year), to = max(Year))


############################
## BEFORE AND AFTER PLOTS ##
############################

time_series_plots = function(filename, economy_codes) {
  input_data %>%
    filter(Economy_Code %in% economy_codes) %>%
    ggplot(aes(x = Year, y = Value, color = Economy_Label, linetype = Variable)) +
      geom_line() +
      theme_bw()
  ggsave(file.path(outputdir, paste0(filename, "_before.png")))
  dfgdp %>% 
    filter(Economy_Code %in% economy_codes) %>% 
    ggplot(aes(x = Year, y = Value, color = Economy_Label, linetype = Variable)) +
      geom_line() +
      theme_bw()
  ggsave(file.path(outputdir, paste0(filename, "_after.png")))
}

time_series_plots("tanzania", c(834, 835, 836))
time_series_plots("czechoslovakia", c(200, 203, 703))
time_series_plots("ussr", c(810, 643, 804, 112, 860, 398, 268, "031", 440, 
                            498, 428, 417, 762, "051", 795, 233))
time_series_plots("yugoslavia", c(688, 499, 191, 807, 412, 705, "070", 890, 891))
time_series_plots("serbia_and_montenegro", c(688, 891, 499))
time_series_plots("sudan", c(728, 729, 736))


#################
## GROUP PLOTS ##
#################

dfgdp %>%
  filter(Economy_Label %in% c("Americas", "Northern America", "Latin America and the Caribbean", "Central America", "South America", "Caribbean")) %>% 
  ggplot(aes(x = Year, y = Value, color = Economy_Label, linetype = Variable)) +
  geom_line()

dfgdp %>%
  filter(Economy_Label %in% c("Europe", "Africa", "Asia", "Americas", "Oceania", "World")) %>% 
  ggplot(aes(x = Year, y = Value, color = Economy_Label, linetype = Variable)) +
  geom_line() +
  scale_y_log10()

dfgdp %>%
  filter(str_detect(Economy_Label, ".*Eu.*")) %>% 
  filter(!str_detect(Economy_Label, ".*Am.*")) %>% 
  ggplot(aes(x = Year, y = Value, color = Economy_Label, linetype = Variable)) +
  geom_line()




#############################################
## COMPARE UPDATED AND CURRENT UNCTAD DATA ##
#############################################

labels = read.csv(file.path(datadir, "lab_all.csv")) %>% 
  mutate(Label = replace(Label, Code == 498, "Republic of Moldova")) %>% 
  mutate(Label = replace(Label, Code == 410, "Republic of Korea")) %>% 
  mutate(Label = replace(Label, Code == 890, "Yugoslavia, Soc. Fed. Rep. of"))

unctaddf = read.csv(file.path(datadir, "US.GDPTotal_20250718_104458.csv")) %>% 
  left_join(
    labels, 
    by = join_by(Economy_Label == Label)) %>%  
  select(Code, Economy_Label, Year, US_at_constant_prices_2015_Value, US_at_current_prices_Value) %>% 
  rename(	
    GDP_at_constant_prices_2015 = US_at_constant_prices_2015_Value,
    GDP_at_current_prices = US_at_current_prices_Value) %>% 
  pivot_longer(4:5, names_to = "Variable", values_to = "Value") %>% 
  full_join(
    dfgdp,
    by = join_by(Code == Economy_Code,
                 Year == Year,
                 Variable == Variable),
    suffix = c(".old", ".new")
  ) 

unctaddf %>% 
  filter(str_detect(Economy_Label.old, "Tai"))

plot_by_economy = function(economy_label) {
   unctaddf %>% 
    filter(Economy_Label.old %in% economy_label) %>% 
    pivot_longer(c(Value.old, Value.new), names_to = "Source", values_to = "Value") %>% 
    mutate(Source = factor(Source, levels = c("Value.old", "Value.new"))) %>% 
    ggplot(aes(x=Year, y=Value, linetype=Variable, color=Source)) +
      geom_line(size=1) +
      theme_bw() + 
      labs(title = economy_label) %>% 
    return
}  

Plots <- lapply(sort(unique(unctaddf$Economy_Label.old)), plot_by_economy)
myPlots <- do.call(marrangeGrob, list(grobs=Plots, nrow = 3, ncol = 1))
ggsave(file.path(outputdir, "GDP_comparison_groups.pdf"), myPlots, height = 12, width = 8)

unctaddf %>% 
  filter(str_detect(Economy_Label.old, ".*Europ.*")) %>% 
  distinct(Economy_Label.old)

plot_by_economy("World")
plot_by_economy("Europe")
plot_by_economy("Americas")
plot_by_economy("Africa")
plot_by_economy("Asia") 
plot_by_economy("China")  %>% ggplotly(dynamicTicks = TRUE)
plot_by_economy("Asia") %>% ggplotly
plot_by_economy("Oceania")
plot_by_economy("China")
plot_by_economy("China, Taiwan Province of") %>% ggplotly
plot_by_economy("Serbia and Montenegro") 
plot_by_economy("Federal Republic of Germany") %>% ggplotly(dynamicTicks = TRUE)
plot_by_economy("Indonesia") 
plot_by_economy("Indonesia") %>% ggplotly(dynamicTicks = TRUE)
plot_by_economy("Indonesia (...2002)") 
plot_by_economy("Indonesia (...2002)") %>% ggplotly(dynamicTicks = TRUE)
plot_by_economy("Panama") 
plot_by_economy("Poland") 
plot_by_economy("Panama, excluding Canal Zone") %>% ggplotly(dynamicTicks = TRUE)
plot_by_economy("Yugoslavia, Soc. Fed. Rep. of")  %>% ggplotly(dynamicTicks = TRUE)
plot_by_economy("Pacific Islands, Trust Territory") %>% ggplotly(dynamicTicks = TRUE)
plot_by_economy("Sudan (...2011)") %>% ggplotly(dynamicTicks = TRUE)
plot_by_economy("Netherlands Antilles") %>% ggplotly(dynamicTicks = TRUE)
plot_by_economy("Yemen") %>% ggplotly(dynamicTicks = TRUE)
plot_by_economy("Yemen, Arab Republic") %>% ggplotly(dynamicTicks = TRUE)
plot_by_economy("Yemen, Democratic") %>% ggplotly(dynamicTicks = TRUE)

unctaddf %>% 
  filter(Economy_Label.old == "Poland",
         Variable == "GDP_at_current_prices") %>% 
  mutate(ratio = Value.old/Value.new)






