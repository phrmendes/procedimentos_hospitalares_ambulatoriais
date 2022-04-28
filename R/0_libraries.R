# ------------------- #
# --- BIBLIOTECAS --- #
# ------------------- #

if (!require("pacman")) install.packages("pacman")

if (!require("devtools")) install.packages("devtools")

# install.packages("arrow", repos = "https://packagemanager.rstudio.com/all/__linux__/focal/latest")

pacman::p_load_gh("ipeaGIT/geobr/r-package", install = FALSE)

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
  memoise,
  miniUI,
  parallel,
  lintr,
  install = FALSE
)
