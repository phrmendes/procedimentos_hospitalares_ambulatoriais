# ------------------- #
# --- BIBLIOTECAS --- #
# ------------------- #

if (!require("pacman")) install.packages("pacman")

# install.packages("arrow", repos = "https://packagemanager.rstudio.com/all/__linux__/focal/latest")

pacman::p_load(
  data.table,
  devtools,
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

pacman::p_load_gh("ipeaGIT/geobr/r-package", install = TRUE)
