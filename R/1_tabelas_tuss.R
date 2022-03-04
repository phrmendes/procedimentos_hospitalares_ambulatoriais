# ----------------------------------------- #
# --- CRIAÇÃO DE DATABASE DE DICIONÁRIO --- #
# ----------------------------------------- #

# funções e bibliotecas ---------------------------------------------------

source("R/0_libraries.R")
source("R/0_functions.R")

# importação e limpeza de dicionários -------------------------------------

# carregando tabelas em uma lista

tabelas <- load_data("data/") |>
  purrr::map(
    janitor::clean_names
  )

# renomeando elementos da lista

tabs <- c("19", "20", "22", "63")

for (i in 1:4) names(tabelas)[i] <- glue::glue("tabela_{tabs[i]}")

rm(i)

# juntando dicionários

tabelas <- bind(
  tabelas$tabela_19,
  tabelas$tabela_20,
  tabelas$tabela_22,
  tabelas$tabela_63
) |>
  dplyr::rename(cd_procedimento = codigo_do_termo) |>
  dplyr::mutate(cd_procedimento = as.character(as.integer(cd_procedimento))) |>
  dplyr::distinct(cd_procedimento, .keep_all = T)

# criando base de tabelas

arrow::write_parquet(
  x = tabelas,
  sink = "data/tabelas_tuss.parquet"
)

# deletando arquivos importados

fs::dir_ls("data/", regexp = "tabela_.") |>
  purrr::walk(
    fs::file_delete
  )
