if (!require("pacman")) install.packages("pacman")

# install.packages("arrow", repos = "https://packagemanager.rstudio.com/all/__linux__/focal/latest") # instalação rápida do arrow

pacman::p_load(
  data.table,
  collapse,
  tidyverse,
  glue,
  tidyfast,
  usethis,
  future,
  furrr,
  pbapply,
  janitor,
  styler,
  fs,
  arrow,
  shiny,
  shinydashboard,
  plotly,
  here, # excluir
  ggthemes,
  dashboardthemes, # excluir
  shinydashboardPlus, # excluir
  geobr,
  sf,
  MetBrewer,
  memoise,
  duckdb, # excluir
  DBI,
  miniUI,
  parallel,
  install = F
)
