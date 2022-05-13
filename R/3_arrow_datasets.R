# --------------------------------- #
# --- DATASETS DE PROCEDIMENTOS --- #
# --------------------------------- #

# bibliotecas, funções e parâmetros ---------------------------------------

source("R/0_libraries.R")
source("R/0_functions.R")

# bases -------------------------------------------------------------------

tuss <- arrow::read_parquet("data/tabelas_tuss.parquet") |>
  dtplyr::lazy_dt(key_by = "cd_procedimento")

db <- list(
  base_hosp = fs::dir_ls("output/", regexp = "base_hosp(.*)\\.parquet") |>
    purrr::map_dfr(arrow::read_parquet),
  base_amb = fs::dir_ls("output/", regexp = "base_amb(.*)\\.parquet") |>
    purrr::map_dfr(arrow::read_parquet)
)

db <- purrr::map2(
  .x = db,
  .y = c("hosp", "amb"),
  ~ .x[
    ,
    db := .y
  ][
    ,
    ":="(
      ano = as.integer(ano),
      mes = as.integer(mes)
    )
  ]
) |>
  data.table::rbindlist() |>
  dtplyr::lazy_dt(key_by = "cd_procedimento")

db |>
  dplyr::left_join(
    tuss,
    by = "cd_procedimento"
  ) |>
  dplyr::filter(!is.na(termo) | termo != "sem_info") |>
  dplyr::select(cd_procedimento, termo, ano, db) |>
  dplyr::distinct() |>
  data.table::as.data.table() |>
  arrow::write_dataset(
    "output/db_termos_shiny",
    format = "parquet",
    partitioning = c("db", "ano")
  )

arrow::write_dataset(
  data.table::as.data.table(db),
  "output/db_shiny",
  format = "parquet",
  partitioning = c("db", "tipo")
)

zip::zip(
  zipfile = fs::file_create("output/bkp_db.zip"),
  files = fs::dir_ls("output/", regexp = ".parquet")
)

fs::dir_ls("output/", regexp = "(base|(termos_(hosp|amb)))") |>
  fs::file_delete()
