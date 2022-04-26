# ----------------------------------------- #
# --- CRIAÇÃO DE DATABASE DE DICIONÁRIO --- #
# ----------------------------------------- #

# funções e bibliotecas ---------------------------------------------------

source("R/0_libraries.R")
source("R/0_functions.R")

# importação e limpeza de dicionários -------------------------------------

tabelas <- fs::dir_ls("data/", regexp = "*.csv") |>
  purrr::map(
    ~ vroom::vroom(.x, show_col_types = FALSE, progress = FALSE) |>
      janitor::clean_names() |>
      dplyr::mutate(
        codigo_do_termo = as.numeric(codigo_do_termo),
        termo = stringr::str_to_upper(termo)
      ) |>
      dplyr::select(codigo_do_termo, termo)
  )

tabs <- c("19", "20", "22", "63")

for (i in 1:4) names(tabelas)[i] <- glue::glue("tabela_{tabs[i]}")

rm(i)

tabelas <- bind(
  tabelas$tabela_19,
  tabelas$tabela_20,
  tabelas$tabela_22,
  tabelas$tabela_63
) |>
  dplyr::rename(cd_procedimento = codigo_do_termo) |>
  dplyr::mutate(cd_procedimento = as.character(as.numeric(cd_procedimento))) |>
  dplyr::distinct(cd_procedimento, tabela, .keep_all = TRUE) |>
  dplyr::select(-tabela)

arrow::write_parquet(
  x = tabelas,
  sink = "data/tabelas_tuss.parquet"
)
