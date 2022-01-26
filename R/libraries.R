# install.packages("pacman")

if (!require("pacman")) install.packages("pacman")

pacman::p_load(
  data.table,
  collapse,
  tidyverse,
  glue,
  tidyfast,
  usethis,
  future,
  future.apply,
  furrr,
  pbapply,
  janitor,
  styler,
  fs,
  shiny,
  shinydashboard,
  plotly,
  here,
  ggthemes,
  dashboardthemes,
  shinydashboardPlus,
  geobr,
  sf,
  MetBrewer,
  memoise,
  duckdb,
  DBI,
  miniUI,
  install = F
)
