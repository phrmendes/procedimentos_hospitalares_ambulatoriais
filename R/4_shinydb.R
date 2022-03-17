# ------------------------------------------ #
# --- CRIAÇÃO DE DATABASE PARA SHINY APP --- #
# ------------------------------------------ #

# funções, bibliotecas e parâmetros ---------------------------------------

source("R/0_libraries.R")
source("R/0_functions.R")

# shapefile de estados ----------------------------------------------------

geobr::read_state(
  code_state = "all",
  showProgress = FALSE
) |>
  dplyr::select(code_state, abbrev_state, geom) |>
  dplyr::rename(categoria = abbrev_state) |>
  readr::write_rds("output/geom_ufs.rds")

# databases hospitalares --------------------------------------------------

parametros <- list(
  estados = c("AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"),
  faixas = c("1 a 4", "5 a 9", "10 a 14", "15 a 19", "20 a 29", "30 a 39", "40 a 49", "50 a 59", "60 a 69", "70 a 79", "80 <", "< 1", "N. I."),
  sexos = c("Masculino", "Feminino", "N. I.")
)

estatisticas <- list(
  cols = c("uf_prestador", "faixa_etaria", "sexo"),
  names = paste0("base_hosp_", c("uf", "idade", "sexo"), "_2020"),
  types = c("uf", "idade", "sexo")
)

purrr::walk(
  1:3,
  ~ export_parquet(
    x = estatisticas$cols[.x],
    complete_vars = parametros[[.x]],
    export_name = estatisticas$names[.x],
    type = estatisticas$types[.x],
    db_name = "proc_hosp_db"
  )
)

base_hosp <- purrr::map(
  fs::dir_ls("output/", regexp = "_[0-9]{2}.parquet$"),
  ~ arrow::read_parquet(.x) |>
    dplyr::collect()
) |>
  data.table::rbindlist()

base_hosp[, .(termos = unique(termo))] |>
  arrow::write_parquet("output/termos_hosp_2020.parquet")

fs::dir_ls("output/", regexp = "_[0-9]{2}.parquet$") |>
  fs::file_delete()

arrow::write_parquet(base_hosp, "output/base_hosp_2020.parquet")

# databases ambulatoriais -------------------------------------------------

estatisticas$names <- paste0("base_amb_", c("uf", "idade", "sexo"), "_2020")

purrr::walk(
  1:3,
  ~ export_parquet(
    x = estatisticas$cols[.x],
    complete_vars = parametros[[.x]],
    export_name = estatisticas$names[.x],
    type = estatisticas$types[.x],
    db_name = "proc_amb_db"
  )
)

base_amb <- purrr::map(
  fs::dir_ls("output/", regexp = "_[0-9]{2}.parquet$"),
  ~ arrow::read_parquet(.x) |>
    dplyr::collect()
) |>
  data.table::rbindlist()

base_amb[, .(termos = unique(termo))] |>
  arrow::write_parquet("output/termos_amb_2020.parquet")

fs::dir_ls("output/", regexp = "_[0-9]{2}.parquet$") |>
  fs::file_delete()

arrow::write_parquet(base_amb, "output/base_amb_2020.parquet")
