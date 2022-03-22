# ------------------------------------------------------- #
# --- BASE CONSOLIDADA DE PROCEDIMENTOS AMBULATORIAIS --- #
# ------------------------------------------------------- #

# bibliotecas, funções e parâmetros ---------------------------------------

source("R/0_libraries.R")
source("R/0_functions.R")

# memory.size(max = 10^12)

# definindo termos da buscas no dados abertos -----------------------------

estados <- c("AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RN", "RO", "RR", "RS", "SC", "SE", "TO", "MG", "RJ", "SP")

bases <- c("DET", "CONS")

urls <- purrr::map(
  bases,
  ~ base(
    ano = "2019",
    estado = estados,
    mes = c(paste0("0", 1:9), 10:12),
    base = .x,
    url = "http://ftp.dadosabertos.ans.gov.br/FTP/PDA/TISS/AMBULATORIAL/",
    proc = "AMB"
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

    return("x")
  }
) |>
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

    return("x")
  }
) |>
  purrr::flatten_chr() |>
  length() %>% # referencia o objeto vindo no pipe
  paste0(., " importados.")

# tratando database -------------------------------------------------------

if (!fs::dir_exists("data/proc_amb_db/")) fs::dir_create("data/proc_amb_db/")

det_db <- paste0(fs::dir_ls(path = "data/parquet/", regexp = "*DET.parquet"))

cons_db <- paste0(fs::dir_ls(path = "data/parquet/", regexp = "*CONS.parquet"))

tuss <- arrow::read_parquet("data/tabelas_tuss.parquet") |>
  data.table::as.data.table() # dicionário de termos

pbapply::pblapply(
  1:length(det_db),
  function(i) {
    clean_db(
      path_1 = det_db[i],
      path_2 = cons_db[i],
      termo = tuss
    )

    gc()

    return("Importado.")
  }
)

fs::dir_delete("data/parquet/")
