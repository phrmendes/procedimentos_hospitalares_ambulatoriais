############################################
#### CRIAÇÃO DE DATABASE PARA SHINY APP ####
############################################

# funções, bibliotecas e parâmetros ---------------------------------------

source("R/1_libraries.R")
source("R/2_functions.R")

future::plan(multisession) # multiprocessamento

# shapefile de estados ----------------------------------------------------

geobr::read_state(
  code_state = "all",
  showProgress = FALSE
) |>
  dplyr::select(code_state, abbrev_state, geom) |>
  dplyr::rename(categoria = abbrev_state) |>
  saveRDS(file = "output/geom_ufs.rds")

# databases hospitalares --------------------------------------------------

shinydb <- duckdb::dbConnect(
  duckdb::duckdb(),
  dbdir = "output/shinydb.duckdb"
)

con <- duckdb::dbConnect(
  duckdb::duckdb(),
  dbdir = "data/proc_hosp.duckdb"
)

# lista de procedimentos disponíveis

termos_hosp <- dplyr::tbl(con, "proc_hosp") |>
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

duckdb::dbWriteTable(
  conn = shinydb,
  value = termos_hosp,
  name = "termos_hosp",
  overwrite = TRUE,
  temporary = FALSE
)

purrr::walk(
  c(con, shinydb),
  ~ duckdb::dbDisconnect(.x, shutdown = TRUE)
)

# estatísticas por estado

estados <- c("AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO")

import_shinydb(
  x = "uf_prestador",
  complete_vars = estados,
  db_name = "base_hosp_uf"
)

# estatísticas por faixa etária

faixas <- c("1 a 4", "5 a 9", "10 a 14", "15 a 19", "20 a 29", "30 a 39", "40 a 49", "50 a 59", "60 a 69", "70 a 79", "80 <", "< 1", "N. I.")

import_shinydb(
  x = "faixa_etaria",
  complete_vars = faixas,
  db_name = "base_hosp_idade"
)

# estatísticas por sexo

import_shinydb(
  x = "sexo",
  complete_vars = c("Masculino", "Feminino", "N. I."),
  db_name = "base_hosp_sexo"
)

# databases ambulatoriais -------------------------------------------------

shinydb <- duckdb::dbConnect(
  duckdb::duckdb(),
  dbdir = "output/shinydb.duckdb"
)

paths <- fs::dir_ls(
  path = "output/",
  regexp = "amb"
) |> paste0()

names <- paths |>
  stringr::str_remove_all("output/") |>
  stringr::str_remove_all("\\.csv")

purrr::walk2(
  paths, names,
  ~ duckdb::duckdb_read_csv(
    conn = shinydb,
    files = .x,
    name = .y
  )
)

paths |>
  purrr::walk(
    fs::file_delete
  )

duckdb::dbDisconnect(shinydb, shutdown = TRUE)

# renomeando colunas ------------------------------------------------------

shinydb <- duckdb::dbConnect(
  duckdb::duckdb(),
  dbdir = "output/shinydb.duckdb"
)

dbs <- duckdb::dbListTables(shinydb) |>
  stringr::str_subset("base")

old_name <- c("tot_qt", "tot_vl", "mean_vl")

new_name <- c("Quantidade total", "Valor total", "Valor médio")

queries <- purrr::map_dfr(
  dbs,
  ~ build_queries(.x, old_name, new_name)
) |> purrr::flatten_chr()

purrr::walk(
  queries,
  ~ DBI::dbExecute(conn = shinydb, statement = .x)
)

duckdb::dbDisconnect(shinydb, shutdown = TRUE)
