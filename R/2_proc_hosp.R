# ------------------------------------------------------ #
# --- BASE CONSOLIDADA DE PROCEDIMENTOS HOSPITALARES --- #
# ------------------------------------------------------ #

# bibliotecas, funções e parâmetros ---------------------------------------

source("R/0_libraries.R")
source("R/0_functions.R")

future::plan("multisession")

ano <- 2020

months <- c(paste0("0", 1:9), 10:12)

# definindo termos da buscas no dados abertos -----------------------------

estados <- readr::read_csv("data/aux_files/estados.csv") |> purrr::flatten_chr()

bases <- c("DET", "CONS")

urls <- purrr::map(
  bases,
  ~ base(
    ano = ano,
    estado = estados,
    mes = meses,
    base = .x,
    url_base = "http://ftp.dadosabertos.ans.gov.br/FTP/PDA/TISS/HOSPITALAR/",
    proc = "HOSP"
  )
)

# download e escrita em database ------------------------------------------

fs::dir_create("data/parquet/")

pbapply::pblapply(
  urls[[1]],
  function(i) {
    unpack_write_parquet(
      url = i,
      cols = c(
        "id_evento_atencao_saude",
        "ano_mes_evento",
        "cd_procedimento",
        "uf_prestador",
        "qt_item_evento_informado",
        "vl_item_evento_informado",
        "ind_tabela_propria"
      )
    )

    gc()

    return("x")
  },
  cl = parallel::detectCores()
)

pbapply::pblapply(
  urls[[2]],
  function(i) {
    unpack_write_parquet(
      url = i,
      cols = c(
        "id_evento_atencao_saude",
        "ano_mes_evento",
        "faixa_etaria",
        "sexo"
      )
    )

    gc()

    return("Importado.")
  },
  cl = parallel::detectCores()
)

# merge entre bases DET e CONS --------------------------------------------

fs::dir_create("data/proc_hosp_db/")

det_db <- fs::dir_ls(path = "data/parquet/", regexp = "*DET.parquet")

cons_db <- fs::dir_ls(path = "data/parquet/", regexp = "*CONS.parquet")

tuss <- arrow::read_parquet("data/tabelas_tuss.parquet")

pbapply::pblapply(
  seq_len(length(det_db)),
  function(i) {
    merge_db(
      path_1 = det_db[i],
      path_2 = cons_db[i],
      termos = tuss
    )

    gc()

    return("Importado.")
  },
  cl = parallel::detectCores()
)

fs::dir_delete("data/parquet/")

# tratando database -------------------------------------------------------

parametros <- list(
  estados = estados,
  faixas = readr::read_csv("data/aux_files/faixas.csv") |> purrr::flatten_chr(),
  sexos = readr::read_csv("data/aux_files/sexos.csv") |> purrr::flatten_chr()
)

estatisticas <- list(
  cols = c("uf_prestador", "faixa_etaria", "sexo"),
  names = glue::glue("base_hosp_{c('uf', 'idade', 'sexo')}_{ano}"),
  types = c("uf", "faixa etária", "sexo")
)

purrr::walk(
  1:3,
  ~ export_parquet(
    x = estatisticas$cols[.x],
    complete_vars = parametros[[.x]],
    export_name = estatisticas$names[.x],
    type = estatisticas$types[.x],
    db_name = "proc_hosp_db",
    months = meses
  )
)

base_hosp <- purrr::map(
  fs::dir_ls("output/", regexp = "_[0-9]{2}.parquet$"),
  ~ arrow::read_parquet(.x) |>
    dplyr::collect()
) |>
  data.table::rbindlist()

base_hosp[, .(termos = unique(termo))] |>
  arrow::write_parquet(glue::glue("output/termos_hosp_{ano}.parquet"))

arrow::write_parquet(base_hosp, glue::glue("output/base_hosp_{ano}.parquet"))

fs::dir_ls("output/", regexp = "_[0-9]{2}.parquet$") |>
  fs::file_delete()

fs::dir_delete("data/proc_hosp_db/")
