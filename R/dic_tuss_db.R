#### Criação de base de dados de dicionário ####

# funções e bibliotecas ---------------------------------------------------

source("R/bibliotecas.R")
source("R/funcoes.R")

# importação e limpeza de dicionários -------------------------------------

# carregando tabelas em uma lista

tabelas <- load_data("tuss-db/") |>
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
)

tabelas <- tabelas |>
  dplyr::rename(cd_procedimento = codigo_do_termo) |>
  data.table::as.data.table()
