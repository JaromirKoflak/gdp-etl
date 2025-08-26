


run_etl_pipeline = function() {

library(tidyverse)
library(readxl)
library(httr)
library(gridExtra)

##############
## SETTINGS ##
##############

last_year = 2024  

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

##################
## ETL PIPELINE ##
##################

read_usis <- function(series, source, measure) {
  paste0(
    "https://usis.unctad.unctad.org/UsisDWDataService/",
    "Series", series, "Source", source, "Measure", measure,
    "FrequencyA/GetLastVersion()/Data?$format=csv"
    ) %>% 
    read_csv(show_col_types = F) %>% 
    return 
}

get_unsd_gdp_data = function() {
  # API calls
  url1 <- "https://unstats.un.org/unsd/amaapi/api/file/6"
  GET(url1, write_disk(tf <- tempfile(fileext = ".xlsx")))
  gdp_constant <- read_excel(tf, skip = 2) %>%
    filter(IndicatorName == "Gross Domestic Product (GDP)") %>%
    pivot_longer(4:57, names_to = "Year", values_to = "GDP_at_constant_prices_2015")

  url2 <- "https://unstats.un.org/unsd/amaapi/api/file/2"
  GET(url2, write_disk(tf <- tempfile(fileext = ".xlsx")))
  gdp_current <- read_excel(tf, skip = 2) %>%
    filter(IndicatorName == "Gross Domestic Product (GDP)") %>%
    pivot_longer(4:57, names_to = "Year", values_to = "GDP_at_current_prices")
  
  # gdp_constant <- read_usis("5100", "4805", "0940")
  # 
  # gdp_current <- read_usis("5100", "4805", "0100")
  
  df <- inner_join(
      gdp_constant, 
      gdp_current,
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

get_taiwan_gdp_data = function(df) {
  ### Taiwan NSO, UNIT: Million NT$
  # Fernando's Code
  t_1970 <- (1970 - 1951) * 100 + 4000
  t_1980 <- (1980 - 1951) * 100 + 4000
  t_1981 <- (1981 - 1951) * 100 + 4000
  t_last <- (last_year - 1 - 1951) * 100 + 4000  # The last year is estimated using growth rates, so we only ask for data up to the penultimate year
  
  # 1.1 Principal Figures 
  # Used for Exchange rates
  url <- paste0("https://nstatdb.dgbas.gov.tw/dgbasall/webMain.aspx?sys=220&funid=E018101010&outmode=3&cycle=4&outkind=3&compmode=0&ratenm=Value&fldlst=111111111111111&compmode=0",
                "&ymf=", t_1970, "&ymt=", t_last, "&rdm=R164860&eng=1")
  temp <- read_csv(url, skip = 2, show_col_types = F)
  data1 <- temp %>% slice(-c((nrow(.)-2):nrow(.)))
  
  # 2.1 Expenditures on GDP Annual & Quarterly (1951-1980)
  # Using only Annual data
  url <- paste0("https://nstatdb.dgbas.gov.tw/dgbasall/webMain.aspx?sys=220&funid=E018102010&outmode=3&cycle=4&outkind=3&compmode=0&ratenm=Value&fldlst=111&codlst0=100000000000011001001001111&compmode=0",
                "&ymf=", t_1970, "&ymt=", t_1980, "&rdm=R63170&eng=1") 
  temp <- read_csv(url, skip = 2, show_col_types = F, na="--", 
                   col_types = cols(
                     `At Current Prices` = col_double(),
                     `Chained (2021) Dollars` = col_double()))
  data2 <- temp %>% slice(-c((nrow(.)-5):nrow(.)))
  
  # 2.2 Expenditures on GDP Annual (since 1981)
  url <- paste0("https://nstatdb.dgbas.gov.tw/dgbasall/webMain.aspx?sys=220&funid=E018102050&outmode=3&cycle=4&outkind=3&compmode=0&ratenm=Value&fldlst=111&codlst0=111001001001111111111&compmode=0",
                "&ymf=", t_1981, "&ymt=", t_last, "&rdm=R23908&eng=1")
  temp <- read_csv(url, skip = 2, show_col_types = F, na="--", 
                   col_types = cols(
                     `At Current Prices` = col_double(),
                     `Chained (2021) Dollars` = col_double()))
  data3 <- temp %>% slice(-c((nrow(.)-3):nrow(.)))
  
  rebase_factor = data3 %>%
    filter(Period == 2015, Expenditure == "8. GDP") %>%
    summarise(rebase_factor = `At Current Prices` / `Chained (2021) Dollars`) %>%
    pull
  
  nsodata = data2 %>%
    add_row(data3) %>% 
    filter(Expenditure %in% c("6.GDP", "8. GDP")) %>% 
    left_join(data1,
              by = join_by(Period)) %>% 
    mutate(Exchange_rate = `GDP (Million N.T.$,at Current Prices)` / `GDP (Million U.S.$,at Current Prices)`) %>% 
    transmute(
      Economy_Code = "158",
      Year = Period,
      # Constant prices are calculated using the exchange rate of the base year for all years  
      GDP_at_constant_prices_2015 = `Chained (2021) Dollars` * 1e6 / Exchange_rate[Period==2015] * rebase_factor,
      # Current prices are calculated using the exchange rate of that year  
      GDP_at_current_prices = `At Current Prices` * 1e6 / Exchange_rate,
    ) %>%
    pivot_longer(-c(1:2), names_to = "Variable", values_to = "Value") %>%
    mutate(Year = as.numeric(Year)) 
  
  return(df %>% bind_rows(nsodata))
}

compute_missing_values = function(df) {
  df %>% 
    # 1 United Republic of Tanzania  1970  2023
    # URT 834 <- Tanzania Mainland 835 + Zanzibar 836
    mutate(Economy_Code=replace(Economy_Code,
                                Economy_Code %in% c(835, 836) & Year %in% c(1970:2023),
                                834)) %>% 
    
    # 2 Czechoslovakia (Former)      1990  1992 
    # Czechoslovakia 200 <- Czechia 203 + Slovakia 703
    mutate(Economy_Code=replace(Economy_Code,
                                Economy_Code %in% c(203, 703) & Year %in% c(1991, 1992),
                                200)) %>% 
    
    # 3 Sudan (Former)               2011  2011
    # Former Sudan 736 <- South Sudan 728 + Sudan 729
    mutate(Economy_Code=replace(Economy_Code,
                                Economy_Code %in% c(728, 729) & Year == 2011,
                                736)) %>% 
    
    # 4 Serbia and Montenegro        1992  1998
    # Serbia and Montenegro 891 <- Serbia 688 + Montenegro 499 
    #   Serbia and Montenegro        1999  2007
    # Serbia and Montenegro 891 <- Serbia 688 + Montenegro 499 + Kosovo 412
    mutate(Economy_Code=replace(Economy_Code,
                                (Economy_Code %in% c(688, 499) & Year %in% c(1992:1998)) |
                                  (Economy_Code %in% c(688, 499, 412) & Year %in% c(1999:2007)),
                                891)) %>% 
    
    # 5 Yugoslavia (Former)          1991  1991
    # Yugoslavia 890 <- Serbia 688 + Montenegro 499 + Croatia 191 + North Macedonia 807 
    #                   + Slovenia 705 + Bosnia and Herzegovina 070
    mutate(Economy_Code=replace(Economy_Code,
                                Economy_Code %in% c(688, 499, 191, 807, 705, "070") & Year == 1991,
                                890)) %>%
    
    # 6 USSR (Former)                1991  1991 
    # USSR 810 <- Russian Federation 643 + Ukraine 804 + Belarus 112 + Uzbekistan 860 + Kazakhstan 398 
    #           + Georgia 268 + Azerbaijan 031 + Lithuania 440 + Moldova 498 + Latvia 428 + Kyrgyzstan 417 
    #           + Tajikistan 762 + Armenia 051 + Turkmenistan 795 + Estonia 233 
    mutate(Economy_Code=replace(Economy_Code,
                                Economy_Code %in% c(643, 804, 112, 860, 398, 268, "031", 440, 
                                                    498, 428, 417, 762, "051", 795, 233) &
                                  Year == 1991,
                                810)) %>%
    
    # 7 Pacific Islands, Trust Ter.  1970  1981
    # Pacific Islands, Trust Ter. 582 <- Micronesia 583 + Marshall Islands 584 + Palau 585
    mutate(Economy_Code=replace(Economy_Code,
                                Economy_Code %in% c(583, 584, 585) & Year %in% c(1970:1981),
                                582)) %>% 
    
    # 8 Federal Republic of Germany  1970  1989
    mutate(Economy_Code=replace(Economy_Code,
                                Economy_Code == 276 & Year %in% c(1970:1989),
                                280)) %>%
    
    # 9 Indonesia (..2002)           1970  2002
    mutate(Economy_Code=replace(Economy_Code,
                                Economy_Code == 360 & Year %in% c(1970:2002),
                                960)) %>%
    
    # 10 Panama, excl. Canal Zone     1970  1980
    mutate(Economy_Code=replace(Economy_Code,
                                Economy_Code == 591 & Year %in% c(1970:1980),
                                590)) %>%
    
    group_by(Economy_Code, Year, Variable) %>%
    summarise_at(vars(Value), sum, na.rm=TRUE) %>%
    ungroup() %>% 
    mutate(Year = as.numeric(Year)) %>% 
    return
}

get_gdp_deflators = function() {
  
  exchange_rates <- read_csv(
    paste0("https://usis.unctad.unctad.org/UsisDWDataService/",
           "Series", 5201, "Source", "0101", "Measure", "0200",
           "FrequencyA/GetLastVersion()/Data?$format=csv"
    ),
    show_col_types = F)
  
  gdp_deflators <- read_csv(
    paste0("https://usis.unctad.unctad.org/UsisDWDataService/",
           "Series", 5105, "Source", "2304", "Measure", 6700,
           "FrequencyA/GetLastVersion()/Data?$format=csv"
    ),
    show_col_types = F)
  
  gdp_deflators %>% 
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
    mutate(Deflator_USD = 100 * Deflator_exg / Deflator2015) %>% 
    return
}

estimate_last_year = function(df) {
  estimate_constant = df %>% 
    filter(Year == last_year-1,
           Variable == "GDP_at_constant_prices_2015") %>%
    left_join(
      read_excel(file.path(datadir, "GDP growth rates.xlsx")) %>% 
        mutate(UNCTcc = recode(
          UNCTcc,
          "842" = "840",
          "926" = "826",
          "757" = "756", 
          "251" = "250", 
          "579" = "578" 
        )) %>% 
        select(UNCTcc, last_col()),
      by = join_by(Economy_Code == UNCTcc)
    ) %>% 
    mutate(
      Year = last_year,
      Value = Value * (1+across(last_col())[[1]]/100)
    ) %>% 
    select(!last_col())
  
  deflator_USD = get_gdp_deflators()
  
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
           Value = Value * Deflator_USD / 100) %>% 
    select(!Deflator_USD)
  
  return(df %>% bind_rows(estimate_constant, estimate_current))
}

round_values = function(df) {
  df %>% 
    mutate(Value = round(Value)) %>% 
    return
}

delete_data_out_of_valid_range = function(df) {
  df %>% 
    left_join(
      dim_countries %>% select(!c(Label, IsTarget)),
      by=join_by(Economy_Code == Code)
    ) %>% 
    filter(between(Year, ValidFrom, ValidTo)) %>% 
    select(!c(ValidTo, ValidFrom)) %>% 
    return
}

add_economy_labels = function(df) {
  df %>% 
    left_join(
      labels,
      by = join_by(Economy_Code == Code)
    ) %>% 
    rename(Economy_Label = Label) %>% 
    select(Economy_Code, Economy_Label, everything()) %>% 
    return
}

expand_hierarchy = function(df) {
  # Number of rows, which can be expanded
  expandable_rows = df %>%
    filter(Child_Code %in% Parent_Code) %>% 
    dim %>% 
    .[1]
  
  # If all groups are already expanded, return df
  if(expandable_rows == 0) {
    return(df)
  }
  
  original_colnames = colnames(df)
  
  # Otherwise expand all groups and recursively call expand_hierarchy
  df %>% 
    left_join(
      df,
      by = join_by(x$Child_Code == y$Parent_Code),
      suffix = c("", ".new"),
      relationship = "many-to-many"
    ) %>% 
    mutate(Child_Code      = ifelse(is.na(Child_Code.new),
                                    Child_Code,
                                    Child_Code.new),
           Child_Label     = ifelse(is.na(Child_Label.new),
                                    Child_Label,
                                    Child_Label.new),
           Child_ValidFrom = ifelse(is.na(Child_ValidFrom.new),
                                    Child_ValidFrom,
                                    Child_ValidFrom.new),
           Child_ValidTo   = ifelse(is.na(Child_ValidTo.new),
                                    Child_ValidTo,
                                    Child_ValidTo.new),
           Child_IsTarget  = ifelse(is.na(Child_IsTarget.new),
                                    Child_IsTarget,
                                    Child_IsTarget.new)) %>% 
    filter(Child_IsTarget == "True" | !is.na(Child_Code.new) | Parent_Label == "Other territories") %>% 
    select(all_of(original_colnames)) %>% 
    expand_hierarchy %>%
    return
}

compute_aggregate_values = function(df) {
  groupgdp = economy_groups %>%
    expand_hierarchy %>% 
    left_join(
      df,
      by = join_by(Child_Code == Economy_Code),
      relationship = "many-to-many"
    ) %>% 
    group_by(Parent_Code, Parent_Label, Year, Variable) %>% 
    summarise(Value=sum(Value, na.rm=TRUE)) %>% 
    ungroup %>% 
    rename(Economy_Code = Parent_Code,
           Economy_Label = Parent_Label) %>% 
    filter(!is.na(Year)) %>% 
    mutate(Economy_Code = replace(Economy_Code,
                                  Economy_Label == "World",
                                  "0000")
    )
  return(
    df %>% 
      bind_rows(groupgdp) %>% 
      arrange(Economy_Code, Year, Variable)
  )
}

add_comments = function(df) {
  df %>% 
    mutate(CommentEN = NA,
           CommentFR = NA) %>% 
    # 1 United Republic of Tanzania
    # URT 834 <- Tanzania Mainland 835 + Zanzibar 836
    mutate(CommentEN=replace(CommentEN,
                             Economy_Code == 834,
                             "Tanzania Mainland 835 + Zanzibar 836"),
           CommentFR=replace(CommentFR,
                             Economy_Code == 834,
                             "Tanzanie continentale 835 + Zanzibar 836")) %>% 
    
    # 2 Czechoslovakia (Former)      1990  1992 
    # Czechoslovakia 200 <- Czechia 203 + Slovakia 703
    mutate(CommentEN=replace(CommentEN,
                             Economy_Code == 200 & Year %in% c(1991, 1992),
                             "Czechia 203 + Slovakia 703"),
           CommentFR=replace(CommentFR,
                             Economy_Code == 200 & Year %in% c(1991, 1992),
                             "Tchéquie 203 + Slovaquie 703")) %>%  
    
    # 3 Sudan (Former)               2011  2011
    # Former Sudan 736 <- South Sudan 728 + Sudan 729
    mutate(CommentEN=replace(CommentEN,
                             Economy_Code == 736 & Year == 2011,
                             "South Sudan 728 + Sudan 729"),
           CommentFR=replace(CommentFR,
                             Economy_Code == 736 & Year == 2011,
                             "Soudan du Sud 728 + Soudan 729")) %>%  
    
    # 4 Serbia and Montenegro        1992  1998
    # Serbia and Montenegro 891 <- Serbia 688 + Montenegro 499 
    mutate(CommentEN=replace(CommentEN,
                             Economy_Code == 891 & Year %in% c(1992:1998),
                             "Serbia 688 + Montenegro 499"),
           CommentFR=replace(CommentFR,
                             Economy_Code == 891 & Year %in% c(1992:1998),
                             "Serbie 688 + Monténégro 499")) %>%  
    
    #   Serbia and Montenegro        1999  2007
    # Serbia and Montenegro 891 <- Serbia 688 + Montenegro 499 + Kosovo 412
    mutate(CommentEN=replace(CommentEN,
                             Economy_Code == 891 & Year %in% c(1999:2007),
                             "Serbia 688 + Montenegro 499 + Kosovo 412"),
           CommentFR=replace(CommentFR,
                             Economy_Code == 891 & Year %in% c(1999:2007),
                             "Serbie 688 + Monténégro 499 + Kosovo 412")) %>%  
    
    # 5 Yugoslavia (Former)          1991  1991
    # Yugoslavia 890 <- Serbia 688 + Montenegro 499 + Croatia 191 + North Macedonia 807 
    #                   + Slovenia 705 + Bosnia and Herzegovina 070
    mutate(CommentEN=replace(CommentEN,
                             Economy_Code == 890 & Year == 1991,
                             "Serbia 688 + Montenegro 499 + Croatia 191 + North Macedonia 807 + Slovenia 705 + Bosnia and Herzegovina 070"),
           CommentFR=replace(CommentFR,
                             Economy_Code == 890 & Year == 1991,
                             "Serbie 688 + Monténégro 499 + Croatie 191 + Macédoine du Nord 807 + Slovénie 705 + Bosnie-Herzégovine 070")) %>% 
    
    # 6 USSR (Former)                1991  1991 
    # USSR 810 <- Russian Federation 643 + Ukraine 804 + Belarus 112 + Uzbekistan 860 + Kazakhstan 398 
    #           + Georgia 268 + Azerbaijan 031 + Lithuania 440 + Moldova 498 + Latvia 428 + Kyrgyzstan 417 
    #           + Tajikistan 762 + Armenia 051 + Turkmenistan 795 + Estonia 233 
    mutate(CommentEN=replace(CommentEN,
                             Economy_Code == 810 & Year == 1991,
                             "Russian Federation 643 + Ukraine 804 + Belarus 112 + Uzbekistan 860 + Kazakhstan 398 + Georgia 268 + Azerbaijan 031 + Lithuania 440 + Moldova 498 + Latvia 428 + Kyrgyzstan 417 + Tajikistan 762 + Armenia 051 + Turkmenistan 795 + Estonia 233"),
           CommentFR=replace(CommentFR,
                             Economy_Code == 810 & Year == 1991,
                             "Fédération de Russie 643 + Ukraine 804 + Bélarus 112 + Ouzbékistan 860 + Kazakhstan 398 + Géorgie 268 + Azerbaïdjan 031 + Lituanie 440 + République de Moldova 498 + Lettonie 428 + Kirghistan 417 + Tadjikistan 762 + Arménie 051 + Turkménistan 795 + Estonie 233")) %>%  
    
    # 7 Pacific Islands, Trust Ter.  1970  1981
    # Pacific Islands, Trust Ter. 582 <- Micronesia 583 + Marshall Islands 584 + Palau 585
    mutate(CommentEN=replace(CommentEN,
                             Economy_Code == 582 & Year %in% c(1970:1981),
                             "Micronesia 583 + Marshall Islands 584 + Palau 585"),
           CommentFR=replace(CommentFR,
                             Economy_Code == 582 & Year %in% c(1970:1981),
                             "Micronésie (États fédérés de) 583 + Îles Marshall 584 + Palaos 585")) %>% 
    
    # 8 Federal Republic of Germany  1970  1989
    # mutate(CommentEN=replace(CommentEN,
    #                          Economy_Code == 280 & Year %in% c(1970:1989),
    #                          "Germany 276"),
    #        CommentFR=replace(CommentFR,
    #                          Economy_Code == 280 & Year %in% c(1970:1989),
    #                          "Allemagne 276")) %>% 
    
    # 9 Indonesia (..2002)           1970  2002
    # mutate(CommentEN=replace(CommentEN,
    #                          Economy_Code == 960 & Year %in% c(1970:2002),
    #                          "Indonesia 360"),
    #        CommentFR=replace(CommentFR,
    #                          Economy_Code == 960 & Year %in% c(1970:2002),
    #                          "Indonésie 360")) %>% 
    
    # 10 Panama, excl. Canal Zone     1970  1980
    # mutate(CommentEN=replace(CommentEN,
    #                          Economy_Code == 590 & Year %in% c(1970:1980),
    #                          "Panama 591"),
    #        CommentFR=replace(CommentFR,
    #                          ,
    #                          "Panama 591")) %>%  
  return
}

export_to_generic_csv = function(df, filename) {
  write_csv(df, file.path(outputdir, filename))
  return(df)
}

export_to_usis_csv = function(df, filename) {
  df %>% 
    filter(str_length(Economy_Code) < 4) %>% 
    transmute(
      Series = 5100,
      Country = Economy_Code,
      Year = Year,
      Period = "A00",
      NAComponent = "00",
      Measure = recode(
        Variable,
        "GDP_at_current_prices" = "0100",
        "GDP_at_constant_prices_2015" = "0940"
      ),
      Source = "0101",
      DataSource = case_when(
        Year == 2024 ~ "0100",
        Economy_Code == "158" ~ "3001",
        .default = "4809"),
      Value = Value,
      DataStatus = "00",
      DataConfidentiality = "0",
      CommentEN = CommentEN,
      CommentFR = CommentFR,
      CommentConfidentiality = "0",
      RefDate = paste(
        day(today()), 
        month(today(), label = T, abbr = T), 
        str_sub(year(today()), 3, 4), 
        sep = "-") # Example date: 13-Aug-25
    ) %>% 
    write_csv(file.path(outputdir, filename))
  return(df)
}

dfgdp = get_unsd_gdp_data() %>% 
  get_taiwan_gdp_data() %>% 
  compute_missing_values() %>%
  estimate_last_year() %>% 
  round_values() %>%
  delete_data_out_of_valid_range() %>%
  add_economy_labels() %>%
  compute_aggregate_values() %>% 
  add_comments() %>%
  export_to_generic_csv("gdp_update.csv") %>% 
  export_to_usis_csv("gdp_update_usis.csv") %>% 
  return
}

dfgdp = run_etl_pipeline()
dfgdp %>% 
  distinct(Economy_Code) %>% 
  tail(80) %>% 
  print(n=80)
