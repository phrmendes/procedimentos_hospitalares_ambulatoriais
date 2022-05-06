# --------------------------------- #
# --- DATASETS DE PROCEDIMENTOS --- #
# --------------------------------- #

# bibliotecas, funções e parâmetros ---------------------------------------

source("R/0_libraries.R")
source("R/0_functions.R")

# bases -------------------------------------------------------------------

db <- list(
  base_hosp = fs::dir_ls("output/", regexp = "base_hosp(.*)\\.parquet") |>
    purrr::map_dfr(arrow::read_parquet) |>
    dplyr::mutate(db = "hosp"),
  base_amb = fs::dir_ls("output/", regexp = "base_amb(.*)\\.parquet") |>
    purrr::map_dfr(arrow::read_parquet) |>
    dplyr::mutate(db = "amb")
)

db <- purrr::map2(
  .x = db,
  .y = c("hosp", "amb"),
  ~ .x[, db := .y]
) |>
  data.table::rbindlist()

termos_db <- list(
  purrr::map2_dfr(
    .x = fs::dir_ls("output/", regexp = "termos_hosp"),
    .y = c(2018, 2019, 2020),
    ~ arrow::read_parquet(.x) |>
      dplyr::mutate(ano = .y, db = "hosp")
  ),
  purrr::map2_dfr(
    .x = fs::dir_ls("output/", regexp = "termos_amb"),
    .y = c(2018, 2019, 2020),
    ~ arrow::read_parquet(.x) |>
      dplyr::mutate(ano = .y, db = "amb")
  )
) |>
  data.table::rbindlist()

db <- db[
  ,
  mes_ano := paste0("1-", mes, "-", ano)
][
  ,
  !c("mes", "ano")
]

termos_db <- termos_db[
  ,
  ano :=  paste0("1-1-", ano)
][
  termos != "sem_info"
]

arrow::write_dataset(
  db,
  "output/db_shiny",
  format = "parquet",
  partitioning = c("db", "tipo")
)

arrow::write_dataset(
  termos_db,
  "output/db_termos_shiny",
  format = "parquet",
  partitioning = c("db", "ano")
)

fs::dir_ls("output/", regexp = "(base|(termos_(hosp|amb)))") |>
  fs::file_delete()
