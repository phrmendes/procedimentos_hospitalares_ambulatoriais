# ------------------------------------------------------ #
# --- BASE CONSOLIDADA DE PROCEDIMENTOS HOSPITALARES --- #
# ------------------------------------------------------ #

# bibliotecas, funções e parâmetros ---------------------------------------

source("R/0_libraries.R")
source("R/0_functions.R")

# memory.size(max = 10^12)

# definindo termos da buscas no dados abertos -----------------------------

estados <- c("AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO")

bases <- c("DET", "CONS")

urls <- purrr::map(
  bases,
  ~ base(
    ano = "2020",
    estado = estados,
    mes = c(paste0("0", 1:9), 10:12),
    base = .x,
    url = "http://ftp.dadosabertos.ans.gov.br/FTP/PDA/TISS/HOSPITALAR/",
    proc = "HOSP"
  )
)

# download e escrita em database ------------------------------------------

if (!fs::dir_exists("data/parquet/")) fs::dir_create("data/parquet/")

pbapply::pblapply(
  1:nrow(urls[[1]]),
  function(i) {
    unpack_write_parquet(
      url = urls[[1]]$url[i],
      mes_url = urls[[1]]$mes[i],
      cols = c(
        "cd_procedimento",
        "id_evento_atencao_saude",
        "uf_prestador",
        "cd_tabela_referencia",
        "qt_item_evento_informado",
        "vl_item_evento_informado"
      ),
      indexes = list(
        "id_evento_atencao_saude",
        "mes",
        "cd_procedimento",
        "uf_prestador"
      )
    )

    gc()

    return("importado")
  },
  cl = parallel::detectCores()
) |> # DET
  purrr::flatten_chr() |>
  length() %>% # referencia o objeto vindo no pipe
  paste0(., " importados.")

pbapply::pblapply(
  1:nrow(urls[[2]]),
  function(i) {
    unpack_write_parquet(
      url = urls[[2]]$url[i],
      mes_url = urls[[2]]$mes[i],
      cols = c(
        "id_evento_atencao_saude",
        "faixa_etaria",
        "sexo"
      ),
      indexes = list(
        "id_evento_atencao_saude",
        "mes",
        "cd_procedimento",
        "uf_prestador"
      )
    )

    gc()

    return("importado")
  },
  cl = parallel::detectCores()
) |> # CONS
  purrr::flatten_chr() |>
  length() %>% # referencia o objeto vindo no pipe
  paste0(., " importados.")

# tratando database -------------------------------------------------------

det_db <- fs::dir_ls(path = "data/parquet/", regexp = "*DET.parquet") |>
  arrow::open_dataset()

cons_db <- fs::dir_ls(path = "data/parquet/", regexp = "*CONS.parquet") |>
  arrow::open_dataset()

tuss <- arrow::open_dataset("data/tabelas_tuss.parquet") # dicionário de termos

final_db <- det_db |>
  dplyr::left_join(
    cons_db,
    by = c("id_evento_atencao_saude", "mes")
  ) |>
  dplyr::left_join(
    tuss,
    by = "cd_procedimento"
  ) |>
  dplyr::group_by(uf_prestador)

if (!fs::dir_exists("data/proc_hosp_db/")) fs::dir_create("data/proc_hosp_db/")

arrow::write_dataset(
  dataset = final_db,
  path = "data/proc_hosp_db/",
  format = "parquet",
  basename_template = paste0("proc_hosp_{i}.parquet")
)

fs::dir_delete("data/parquet/")
