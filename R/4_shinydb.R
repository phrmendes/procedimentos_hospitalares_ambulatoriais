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

    return("Exportado!")
  }
)

# procedimentos excedentes e lista de procedimentos disponíveis -----------

shinydb <- purrr::map(
  fs::dir_ls("output/", regexp = "base(.*)parquet"),
  arrow::read_parquet
)

x <- c(2, 3, 5, 6)
y <- c(1, 1, 4, 4)

for (i in 1:4) {
  shinydb[[x[i]]] <- shinydb[[x[i]]] |> dplyr::semi_join(shinydb[[y[i]]], by = "cd_procedimento")
} # filtra procedimentos excedentes em outras bases

shinydb[[3]] |>
  dplyr::distinct(termo) |>
  arrow::write_parquet("output/termos_amb.parquet")

shinydb[[6]] |>
  dplyr::distinct(termo) |>
  arrow::write_parquet("output/termos_hosp.parquet")

purrr::walk2(
  .x = shinydb,
  .y = names(shinydb),
  ~ arrow::write_parquet(.x, glue::glue("{.y}"))
)
