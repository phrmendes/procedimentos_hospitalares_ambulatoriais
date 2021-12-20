# install.packages("pacman")

library("pacman")

pacman::p_load(data.table, 
               collapse, 
               tidyverse,
               glue, 
               tidyfast, 
               usethis, 
               future, 
               future.apply, 
               furrr, 
               janitor, 
               styler,
               fs,
               shiny,
               shinydashboard,
               plotly,
               here,
               devtools,
               dashboardthemes,
               shinydashboardPlus,
               install = F)

# devtools::install_github("BlakeRMills/MetBrewer") 