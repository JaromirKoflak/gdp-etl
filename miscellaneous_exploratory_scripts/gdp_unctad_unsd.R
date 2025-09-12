###############################################################
## GDP and GDP per capita comparison of UNCTAD and UNSD data ##
###############################################################

library(readr)
library(readxl)
library(ggplot2)
library(reshape2)
library(dplyr)
library(tidyr)
library(systemfonts)
library(stringr)
library(gridExtra)

typefaces = system_fonts()
typefaces %>% 
  filter(family == "Roboto") %>% 
  glimpse

##############
## SETTINGS ##
##############

### Getting the path of your current open file
current_path = rstudioapi::getActiveDocumentContext()$path 
setwd(dirname(current_path))
print(getwd())

datadir = paste(getwd(), "/data", sep = "")
outputdir = paste(getwd(), "/output", sep = "")
FirstYear = 1970
LastYear = 2024
GDPPCrange = c(50, 3e5)
GDPrange = c(5e6, 1e14) 
Yearrange = c(FirstYear, LastYear)
un_color_palette = c("#009EDB", "#72BF44", "#FFC800", "#F58220", "#ED1847", "#A05FB4", "#AEA29A")


# Growth rate for estimating GDP in 2024
growth_rate = read_excel(file.path(datadir, "GDP growth rates.xlsx")) %>%
  mutate(UNCTcc = as.numeric(UNCTcc))


##################################
## UN STATISTICAL DIVISION DATA ##
##################################

# GDP 
unsddf = read_excel(paste(datadir, "Download-GDPconstant-USD-countries.xlsx", sep="/"), skip = 2) %>%
  filter(IndicatorName == "Gross Domestic Product (GDP)") %>%
  left_join(
    growth_rate %>%
      mutate(gr2024 = `2024`) %>%
      select(UNCTcc, gr2024),
    by = c("CountryID" = "UNCTcc")) %>% 
  mutate(`2024` = .$`2023` * (1 + gr2024/100)) %>%  # Estimate 2024 GDP using growth rate
  select(-c(IndicatorName, gr2024)) %>% 
  pivot_longer(3:57, names_to = "Year", values_to = "gdp_unsd") %>% 
  mutate(Year = as.numeric(Year))

ggplot(unsddf, aes(x=Year, y=gdp_unsd, group=Country)) +
  geom_line(aes(color=Country), alpha = 0.8) +
  scale_y_log10(limits = GDPrange) +
  scale_x_continuous(limits = Yearrange) +
  ylab("GDP") +
  xlab("Year") + 
  ggtitle("GDP, PPP (constant 2015 international $), UNSD data") +
  theme_bw(base_family = "Roboto") +
  theme(legend.position = "none", text = element_text(face = 2)) +
  scale_color_manual(values = rep(un_color_palette, 40))
ggsave(file.path(outputdir, "gdp_unsd.png"))

# GDP per capita 
unsddf_pc = read_excel(paste(datadir, "Download-GDPPCconstant-USD-countries.xlsx", sep="/"), skip = 2) %>% 
  pivot_longer(3:56, names_to = "Year", values_to = "gdppc_unsd") %>%
  mutate(Year = as.numeric(Year)) 
unsddf_pc

ggplot(unsddf_pc, aes(x=Year, y=gdppc_unsd, group=Country)) +
  geom_line(aes(color=Country), alpha = 0.8) +
  scale_y_log10(limits = GDPPCrange) +
  scale_x_continuous(limits = Yearrange) +
  ylab("GDP per capita") +
  xlab("Year") +
  ggtitle("GDP per capita, PPP (constant 2015 international $), UNSD data") +
  theme_bw(base_family = "Roboto") +
  theme(legend.position = "none", text = element_text(face = 2)) +
  scale_color_manual(values = rep(un_color_palette, 40))
ggsave(file.path(outputdir, "gdppc_unsd.png"))


#################
## UNCTAD DATA ##
#################

# Import labels and UNCTAD data
labels = read.csv(file.path(datadir, "lab_all.csv")) %>% 
  mutate(Label = replace(Label, Code == 498, "Republic of Moldova")) %>% 
  mutate(Label = replace(Label, Code == 410, "Republic of Korea")) %>% 
  mutate(Label = replace(Label, Code == 890, "Yugoslavia, Soc. Fed. Rep. of"))

unctaddf = read.csv(file.path(datadir, "US.GDPTotal.csv")) %>% 
  full_join(labels, by = join_by(Economy_Label == Label)) %>%  
  mutate(Code = as.numeric(Code)) %>% 
  filter(Code < 1000, Code > 1) %>% 
  select(Code, everything())

# Check that all economies have been assigned their code
# Should return 0 rows
unctaddf %>% 
  filter(is.na(Code)) %>% 
  distinct(Economy_Label)

# Estimate GDP for the year 2024 using growth rate
year2024 = unctaddf %>%
  filter(Year == 2023) %>%
  select(Code, Economy_Label, Year, US_at_constant_prices_2015_Value) %>%
  left_join(
    growth_rate %>%
      mutate(gr2024 = `2024`) %>%
      select(UNCTcc, gr2024),
    by = join_by(Code == UNCTcc)) %>%
  mutate(Year = 2024,
         US_at_constant_prices_2015_Value = (US_at_constant_prices_2015_Value * (1+gr2024/100)))

# GDP plot
ggplot(unctaddf, aes(x=Year, y=US_at_constant_prices_2015_Value, group=Economy_Label)) +
  geom_line(aes(color=Economy_Label), alpha = 0.8) +
  scale_y_log10(limits=GDPrange) +
  scale_x_continuous(limits = Yearrange) +
  ylab("GDP") +
  xlab("Year") + 
  ggtitle("GDP, PPP (constant 2015 international $), UNCTAD data") +
  theme_bw(base_family = "Roboto") +
  theme(legend.position = "none", text = element_text(face = 2)) +
  scale_color_manual(values = rep(un_color_palette, 50))
ggsave(file.path(outputdir, "gdp_unctad.png"))



# GDP per capita plot
ggplot(unctaddf, aes(x=Year, y=US_at_constant_prices_2015_per_capita_Value, group=Economy_Label)) +
  geom_line(aes(color=Economy_Label), alpha = 0.8) +
  scale_y_log10(limits = GDPPCrange) +
  scale_x_continuous(limits = Yearrange) +
  ylab("GDP per capita") +
  xlab("Year") +
  ggtitle("GDP per capita, PPP (constant 2015 international $), UNCTAD data") +
  theme_bw(base_family = "Roboto") +
  theme(legend.position = "none", text = element_text(face = 2)) +
  scale_color_manual(values = rep(un_color_palette, 50))
ggsave(file.path(outputdir, "gdppc_unctad.png"))

##########################################
## 2024 estimate comparison with UNCTAD ##
##########################################

diffdf2024 = 
  inner_join(year2024,
             unctaddf %>% filter(Year == 2024),
             by = join_by(Code == Code)) %>%
  mutate(diff = .[,"US_at_constant_prices_2015_Value.x"] - .[,"US_at_constant_prices_2015_Value.y"]) %>% 
  arrange(desc(abs(diff)))
diffdf2024 %>% 
  select(Code, Economy_Label.x, Year.x, diff, US_at_constant_prices_2015_Value.x, US_at_constant_prices_2015_Value.y) %>% 
  head

unctaddf %>% filter(Code == 834, Year %in% c(2023, 2024))
growth_rate %>% filter(UNCTcc == 834) %>% select(`2023`, `2024`)


########################
## DATASET COMPARISON ##
########################

compare_long_tables = function(df1, df2, col1, col2, by) {
  diffdf = 
    inner_join(df1, df2, by = by) %>% 
    mutate(diff = {{col1}} - {{col2}}) %>% 
    arrange(desc(abs(diff)))
  return(diffdf)
}


## GDP UNCTAD vs. UNSD

gdp_diffdf = compare_long_tables(unsddf, 
                             unctaddf, 
                             gdp_unsd, 
                             US_at_constant_prices_2015_Value, 
                             by = join_by(Year == Year, CountryID == Code))
gdp_diffdf %>% 
  select(CountryID, Country, Year, diff, gdp_unsd, US_at_constant_prices_2015_Value) %>% 
  print(n=20)

gdp_diffdf %>% 
  filter(round(diff) != 0) %>% 
  ggplot(aes(x = Year, y = diff, color = Country)) +
  geom_line(alpha = 0.8) +
  geom_point(alpha = 0.8) +
  scale_y_continuous() +
  scale_x_continuous(limits = Yearrange) +
  ylab("GDP") +
  xlab("Year") + 
  ggtitle("GDP difference between UNCTAD and UNSD, PPP (constant 2015 international $)") +
  theme_bw(base_family = "Roboto") +
  theme(text = element_text(face = 2)) 
ggsave(file.path(outputdir, "gdp_difference.png"))


## GDP per capita

gdppc_diffdf = compare_long_tables(unsddf_pc, 
                             unctaddf, 
                             gdppc_unsd, 
                             US_at_constant_prices_2015_per_capita_Value, 
                             by = join_by(Year == Year, CountryID == Code))

gdppc_diffdf %>% 
  select(CountryID, Country, Year, diff, gdppc_unsd, US_at_constant_prices_2015_per_capita_Value) %>% 
  print(n=20)

gdppc_diffdf %>% 
  filter(!(Country %in% c("Former Netherlands Antilles", "Yemen Democratic (Former)", "D.P.R. of Korea"))) %>% 
  select(CountryID, Country, Year, diff, gdppc_unsd, US_at_constant_prices_2015_per_capita_Value) %>% 
  print(n=20)

gdppc_diffdf %>% 
  filter(round(diff) != 0) %>% 
  ggplot(aes(x = Year, y = diff, color = Country)) +
    geom_line(alpha = 0.8) +
    geom_point(alpha = 0.8) +
    scale_y_continuous() +
    scale_x_continuous(limits = Yearrange) +
    ylab("GDP per capita") +
    xlab("Year") + 
    ggtitle("GDP per capita difference between UNCTAD and UNSD, PPP (constant 2015 international $)") +
    theme_bw(base_family = "Roboto") +
    theme(text = element_text(face = 2)) 
ggsave(file.path(outputdir, "gdppc_difference.png"))


##################################
## SAVE COMPARISON PLOTS TO PDF ## 
##################################

gdp_pivot = gdp_diffdf %>% 
  arrange(CountryID, Year) %>% 
  mutate(unsd = gdp_unsd, 
         unctad = US_at_constant_prices_2015_Value) %>% 
  select(CountryID, Country, Year, unsd, unctad) %>% 
  pivot_longer(c(unsd, unctad), names_to = "Source", values_to = "Value")
gdp_pivot %>% 
  head

gdppc_pivot = gdppc_diffdf %>% 
  arrange(CountryID, Year) %>% 
  mutate(unsd = gdppc_unsd, 
         unctad = US_at_constant_prices_2015_per_capita_Value) %>% 
  select(CountryID, Country, Year, unsd, unctad) %>% 
  pivot_longer(c(unsd, unctad), names_to = "Source", values_to = "Value")
gdppc_pivot %>% 
  head

plot_values_by_country <- function(df, country, ylimits){
  df %>%
    filter(Country == country) %>%  
    ggplot(aes(x=Year, y=Value, group=Source, color=Source)) +
    geom_line(alpha=0.5, size=1) +
    geom_point(alpha=0.5, size=2.8) +
    scale_y_log10(limits = ylimits) +
    scale_x_continuous(limits = Yearrange) +
    scale_color_manual(values = un_color_palette[c(5,1)]) +
    labs(
      title = country,
      x = "",
      y = "") +
    theme_bw() +
    theme(text = element_text(face = 2))
    # facet_wrap(~`Flow Label`) +
    # labs(title = country, x="", y="") +
    # theme(axis.text = element_text(size = 6),
    #       strip.text = element_text(size = 6))
}

plot_values_by_country(gdp_pivot, country="Czechia", ylimits = GDPrange)
plot_values_by_country(gdp_pivot, country="Czechoslovakia (Former)", ylimits = GDPrange)

Plots <- lapply(sort(unique(gdp_pivot$Country)), function(x) plot_values_by_country(df = gdp_pivot, country = x, ylimits = GDPrange))
myPlots <- do.call(marrangeGrob, list(grobs=Plots, nrow = 3, ncol = 1))
ggsave(file.path(outputdir, "GDP_comparison.pdf"), myPlots, height = 12, width = 8)

Plots <- lapply(sort(unique(gdppc_pivot$Country)), function(x) plot_values_by_country(df = gdppc_pivot, country = x, ylimits = GDPPCrange))
myPlots <- do.call(marrangeGrob, list(grobs=Plots, nrow = 3, ncol = 1))
ggsave(file.path(outputdir,"GDPPC_comparison.pdf"), myPlots, height = 12, width = 8)

#################
## GROUP PLOTS ##
#################


