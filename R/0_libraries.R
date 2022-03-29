# ------------------- #
# --- BIBLIOTECAS --- #
# ------------------- #


if (!require("pacman")) require(install.packages("pacman"))

# install.packages("arrow", repos = "https://packagemanager.rstudio.com/all/__linux__/focal/latest") # instalação rápida do arrow

pacman::p_load(
  data.table,
  collapse,
  tidyverse,
  glue,
  tidyfast,
  usethis,
  furrr,
  pbapply,
  googleComputeEngineR,
  janitor,
  styler,
  fs,
  arrow,
  shiny,
  shinydashboard,
  plotly,
  ggthemes,
  dashboardthemes,
  shinydashboardPlus,
  geobr,
  sf,
  MetBrewer,
  memoise,
  miniUI,
  parallel,
  lintr,
  duckdb,
  install = F
)
