

run_comparison_with_old_data = function() {

library(tidyverse)
library(readxl)
library(httr)
library(gridExtra)

### Getting the path of your current open file
current_path = rstudioapi::getActiveDocumentContext()$path
setwd(dirname(current_path))
print(getwd())

# Directories
datadir = file.path(getwd(), "data")
outputdir = file.path(getwd(), "output")

# New data
dfgdp = read_csv(file.path(outputdir, "gdp_update.csv"), show_col_types = F)

# Economy labels
labels = read.csv(file.path(datadir, "lab_all.csv")) %>% 
  mutate(Label = replace(Label, Code == 498, "Republic of Moldova")) %>% 
  mutate(Label = replace(Label, Code == 410, "Republic of Korea")) %>% 
  mutate(Label = replace(Label, Code == 890, "Yugoslavia, Soc. Fed. Rep. of"))

# Old data
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

# Export to csv
unctaddf %>% 
  mutate(Economy_Label = Economy_Label.new) %>% 
  select(!c(Economy_Label.old, Economy_Label.new)) %>% 
  select(Code, Economy_Label, everything()) %>% 
  write_csv(file.path(outputdir, "gdp_comparison.csv"))

# Create plots
plot_by_economy = function(economy_label) {
  margin_constant = 0.03
    p = unctaddf %>% 
      filter(Economy_Label.old == economy_label) %>% 
      pivot_longer(c(Value.old, Value.new), names_to = "Release", values_to = "Value") %>% 
      mutate(Release = Release %>% 
               fct_recode(old = "Value.old", new = "Value.new") %>% 
               fct_relevel(c("old", "new"))) %>% 
      ggplot(aes(x=Year, y=Value, linetype=Variable, color=Release)) +
      geom_line(linewidth=1) +
      guides(
        color = guide_legend(
          position = "bottom",
          direction = "vertical",
          nrow=2,
          order = 1),
        linetype = guide_legend(
          position = "bottom",
          direction = "vertical",
          nrow=2)
      ) +
      theme_bw() + 
      theme(
        plot.margin = margin(margin_constant, margin_constant, margin_constant, margin_constant, "npc"),
        legend.spacing.x = unit(0.1, "npc")) + 
      labs(title = economy_label,
           x = "",
           y = "USD") +
      scale_color_manual(values = c("#FBAF17", "#009EDB")) 
    return(p)
}  
# plot_by_economy("Poland")

# Export plots to pdf
Plots <- lapply(sort(unique(unctaddf$Economy_Label.old)), plot_by_economy)
myPlots <- do.call(marrangeGrob, list(grobs=Plots, nrow = 3, ncol = 1))
ggsave(file.path(outputdir, "GDP_comparison_groups.pdf"), myPlots, height = 12, width = 8)

}

run_comparison_with_old_data()
