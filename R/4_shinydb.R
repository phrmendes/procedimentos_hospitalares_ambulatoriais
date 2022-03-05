# ------------------------------------------ #
# --- CRIAÇÃO DE DATABASE PARA SHINY APP --- #
# ------------------------------------------ #

# funções, bibliotecas e parâmetros ---------------------------------------

source("R/0_libraries.R")
source("R/0_functions.R")

future::plan(multisession)

# shapefile de estados ----------------------------------------------------

geobr::read_state(
  code_state = "all",
  showProgress = FALSE
) |>
  dplyr::select(code_state, abbrev_state, geom) |>
  dplyr::rename(categoria = abbrev_state) |>
  saveRDS(file = "output/geom_ufs.rds")

# databases hospitalares --------------------------------------------------

# lista de procedimentos disponíveis

termos_hosp <- arrow::open_dataset("data/proc_hosp_db/") |>
  dplyr::select(termo) |>
  dplyr::distinct() |>
  dplyr::collect() |>
  data.table::as.data.table()

termos_hosp[
  ,
  termo := furrr::future_map_chr(
    termo,
    stringr::str_to_upper
  )
]

arrow::write_parquet(termos_hosp, "output/termos_hosp.parquet")

# cálculo de estatísticas

parametros <- list(
  estados = c("AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"),
  faixas = c("1 a 4", "5 a 9", "10 a 14", "15 a 19", "20 a 29", "30 a 39", "40 a 49", "50 a 59", "60 a 69", "70 a 79", "80 <", "< 1", "N. I."),
  sexos = c("Masculino", "Feminino", "N. I.")
)

estatisticas <- list(
  cols = c("uf_prestador", "faixa_etaria", "sexo"),
  names = paste0("base_hosp_", c("uf", "idade", "sexo"))
)

pbapply::pblapply(
  1:3,
  function(i) {
      export_parquet(
      x = estatisticas$cols[i],
      complete_vars = parametros[[i]],
      db_name = "proc_hosp_db",
      export_name = estatisticas$names[i]
      )

    gc()

    return("Importado!")
  }
)

# databases ambulatoriais -------------------------------------------------

# lista de procedimentos disponíveis

termos_amb <- arrow::open_dataset("data/proc_amb_db/") |>
  dplyr::select(termo) |>
  dplyr::distinct() |>
  dplyr::collect() |>
  data.table::as.data.table()

termos_amb[
  ,
  termo := furrr::future_map_chr(
    termo,
    stringr::str_to_upper
  )
]

arrow::write_parquet(termos_amb, "output/termos_amb.parquet")

# cálculo de estatísticas

parametros <- list(
  estados = c("AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"),
  faixas = c("1 a 4", "5 a 9", "10 a 14", "15 a 19", "20 a 29", "30 a 39", "40 a 49", "50 a 59", "60 a 69", "70 a 79", "80 <", "< 1", "N. I."),
  sexos = c("Masculino", "Feminino", "N. I.")
)

estatisticas <- list(
  cols = c("uf_prestador", "faixa_etaria", "sexo"),
  names = paste0("base_amb_", c("uf", "idade", "sexo"))
)

pbapply::pblapply(
  1:3,
  function(i) {
    export_parquet(
      x = estatisticas$cols[i],
      complete_vars = parametros[[i]],
      db_name = "proc_amb_db",
      export_name = estatisticas$names[i]
    )

    gc()

    return("Importado!")
  }
)
