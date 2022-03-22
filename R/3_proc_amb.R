# ------------------------------------------------------- #
# --- BASE CONSOLIDADA DE PROCEDIMENTOS AMBULATORIAIS --- #
# ------------------------------------------------------- #

# bibliotecas, funções e parâmetros ---------------------------------------

source("R/0_libraries.R")
source("R/0_functions.R")

# memory.size(max = 10^12)

ano <- 2019

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

# merge entre bases DET e CONS --------------------------------------------

fs::dir_create("data/proc_amb_db/")

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

# tratando database -------------------------------------------------------

parametros <- list(
  estados = c("AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"),
  faixas = c("1 a 4", "5 a 9", "10 a 14", "15 a 19", "20 a 29", "30 a 39", "40 a 49", "50 a 59", "60 a 69", "70 a 79", "80 <", "< 1", "N. I."),
  sexos = c("Masculino", "Feminino", "N. I.")
)

estatisticas <- list(
  cols = c("uf_prestador", "faixa_etaria", "sexo"),
  names = paste0("base_amb_", c("uf", "idade", "sexo"), "_", ano),
  types = c("uf", "idade", "sexo")
)
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
  arrow::write_parquet(glue::glue("output/termos_amb_{ano}.parquet"))

fs::dir_ls("output/", regexp = "_[0-9]{2}.parquet$") |>
  fs::file_delete()

arrow::write_parquet(base_amb, glue::glue("output/base_amb_{ano}.parquet"))

