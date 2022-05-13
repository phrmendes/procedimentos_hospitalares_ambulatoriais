# --------------------------------- #
# --- DATASETS DE PROCEDIMENTOS --- #
# --------------------------------- #

# bibliotecas, funções e parâmetros ---------------------------------------

source("R/0_libraries.R")
source("R/0_functions.R")

# bases -------------------------------------------------------------------

tuss <- arrow::read_parquet("data/tabelas_tuss.parquet") |>
  data.table::as.data.table(key = "cd_procedimento")

db <- list(
  base_hosp = fs::dir_ls("output/", regexp = "base_hosp(.*)\\.parquet") |>
    purrr::map_dfr(arrow::read_parquet),
  base_amb = fs::dir_ls("output/", regexp = "base_amb(.*)\\.parquet") |>
    purrr::map_dfr(arrow::read_parquet)
)

db <- purrr::map2(
  .x = db,
  .y = c("hosp", "amb"),
  ~ .x[, db := .y]
) |>
  data.table::rbindlist() |>
  data.table::setkey("cd_procedimento")

db <- data.table::merge.data.table(
  db,
  tuss,
  by = "cd_procedimento",
  all.x = TRUE
)

db <- db |>
  dtplyr::lazy_dt() |>
  dplyr::mutate(mes = forcats::as_factor(mes)) |>
  tidyr::complete(
    cd_procedimento, tipo, db, categoria, ano, mes,
    fill = list(
      tot_qt = 0,
      tot_vl = 0,
      mean_vl = 0
    )
  ) |>
  data.table::as.data.table()

db[is.na(termo), termo := "sem_info"]

termos <- db[
  termo != "sem_info",
  c("cd_procedimento", "termo", "ano", "db")
] |>
  collapse::funique(
    cols = c("cd_procedimento", "termo", "ano", "db")
  )

arrow::write_dataset(
  db,
  "output/db_shiny",
  format = "parquet",
  partitioning = c("db", "tipo")
)

arrow::write_dataset(
  termos,
  "output/db_termos_shiny",
  format = "parquet",
  partitioning = c("db", "ano")
)

zip::zip(
  zipfile = fs::file_create("output/bkp_db.zip"),
  files = fs::dir_ls("output/", regexp = ".parquet")
)

fs::dir_ls("output/", regexp = "(base|(termos_(hosp|amb)))") |>
  fs::file_delete()
