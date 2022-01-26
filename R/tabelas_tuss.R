#### criação de base de dados de dicionário ####

# funções e bibliotecas ---------------------------------------------------

source("R/libraries.R")
source("R/functions.R")

# importação e limpeza de dicionários -------------------------------------

# carregando tabelas em uma lista

tabelas <- load_data("input/") |>
  purrr::map(
    janitor::clean_names
  )

# renomeando elementos da lista

tabs <- c("19", "20", "22", "63")

for (i in 1:4) {
  names(tabelas)[i] <- glue::glue("tabela_{tabs[i]}")
}

rm(i)

# juntando dicionários

tabelas <- bind(
  tabelas$tabela_19,
  tabelas$tabela_20,
  tabelas$tabela_22,
  tabelas$tabela_63
) |>
  dplyr::rename(cd_procedimento = codigo_do_termo) |>
  dplyr::mutate(cd_procedimento = as.character(as.integer(cd_procedimento)))

# criando duckdb

con <- duckdb::dbConnect(
  duckdb::duckdb(),
  dbdir = "input/proc_hosp_amb.duckdb")

dplyr::copy_to(
  dest = con,
  df = tabelas,
  name = "tabelas_tuss",
  indexes = list(
    "cd_procedimento",
    "termo"
  ),
  temporary = FALSE,
  overwrite = TRUE
)

DBI::dbDisconnect(con, shutdown = TRUE)
