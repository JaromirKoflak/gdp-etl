# Special Cases

## Taiwan Province of China

The UNSD dataset does not contain data for Taiwan. Taiwan data is extracted separately from [Taiwan's National Statistical Office](https://nstatdb.dgbas.gov.tw/dgbasall/webMain.aspx?k=engmain).

## Former economies

Three former economies were relabeled.

### Federal Republic of Germany  1970-1989 {-}
Federal Republic of Germany 280 <- Germany 276

### Indonesia (..2002)  1970-2002 {-}
Indonesia (..2002) 960 <- Indonesia 360

### Panama, excluding Canal Zone  1970-1980 {-}
Panama, excluding Canal Zone 590 <- Panama 591

## Dissolved Economies

Data for certain dissolved economies had to be calculated by aggregating its descendant economies. 

**Example:** Czechoslovakia has split up in 1993 into Czechia and Slovakia. UNCTAD&nbsp;requires data for Czechoslovakia until 1992 and separate data since 1993. UNSD contains Czechoslovakia data until 1989 and separate data since 1990. The ETL script sums Czechia and Slovakia data for the years 1990-1992 and adds a *Czechoslovakia* label, an economy code *200* and a comment *Czechia 203 + Slovakia 703* referring to the origin of the values.  

Below is the full list of special cases.

### United Republic of Tanzania  1970-2023 {-}
URT 834 <- Tanzania Mainland 835 + Zanzibar 836

### Czechoslovakia (Former)  1990-1992 {-}
Czechoslovakia 200 <- Czechia 203 + Slovakia 703

### Sudan (Former)  2011  {-}
Former Sudan 736 <- South Sudan 728 + Sudan 729

### Serbia and Montenegro  1992-1998 {-}
Serbia and Montenegro 891 <- Serbia 688 + Montenegro 499 

### Serbia and Montenegro  1999-2007 {-}
Serbia and Montenegro 891 <- Serbia 688 + Montenegro 499 + Kosovo 412

### Yugoslavia (Former)  1991  {-}
Yugoslavia 890 <- Serbia 688 + Montenegro 499 + Croatia 191 + North Macedonia 807 
                      + Slovenia 705 + Bosnia and Herzegovina 070

### USSR (Former)  1991  {-}
USSR 810 <- Russian Federation 643 + Ukraine 804 + Belarus 112 + Uzbekistan 860 + Kazakhstan 398 
             + Georgia 268 + Azerbaijan 031 + Lithuania 440 + Moldova 498 + Latvia 428 + Kyrgyzstan 417 
             + Tajikistan 762 + Armenia 051 + Turkmenistan 795 + Estonia 233 
            
### Pacific Islands, Trust Territory  1970-1981 {-}
Pacific Islands, Trust Territory 582 <- Micronesia 583 + Marshall Islands 584 + Palau 585             



