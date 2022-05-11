# ------------------- #
# --- BIBLIOTECAS --- #
# ------------------- #

if (!require("pacman")) {
  install.packages("pacman")
  require("pacman")
}

if (!require("arrow")) {
  install.packages(
    "arrow",
    repos = "https://packagemanager.rstudio.com/all/__linux__/focal/latest"
  )
  require("arrow")
}

pacman::p_load(
  data.table,
  devtools,
  collapse,
  tidyverse,
  glue,
  tidyfast,
  furrr,
  pbapply,
  janitor,
  styler,
  fs,
  miniUI,
  parallel,
  lintr,
  install = FALSE
)
