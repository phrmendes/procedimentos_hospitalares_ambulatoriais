# ------------------- #
# --- BIBLIOTECAS --- #
# ------------------- #

if (!require("pacman")) {
  install.packages("pacman")
  require("pacman")
}

if (!require("arrow")) {
  install.packages("arrow", repos = "https://packagemanager.rstudio.com/all/__linux__/focal/latest")
  require("arrow")
}

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
  memoise,
  miniUI,
  parallel,
  lintr,
  install = FALSE
)

if (!require("geobr")) {
  pacman::p_load_gh("ipeaGIT/geobr/r-package", install = TRUE)
  require("geobr")
}
