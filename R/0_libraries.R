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
  ggthemes,
  dashboardthemes, # excluir
  shinydashboardPlus, # excluir
  geobr,
  sf,
  MetBrewer,
  memoise,
  DBI,
  miniUI,
  parallel,
  install = F
)
