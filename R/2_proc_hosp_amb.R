# ---------------------------------------------------------------------- #
# --- BASE CONSOLIDADA DE PROCEDIMENTOS HOSPITALARES E AMBULATORIAIS --- #
# ---------------------------------------------------------------------- #

# bibliotecas, funções e parâmetros ---------------------------------------

# MUDAR MOMENTO DO JOIN COM TABELA TUSS

source("R/0_libraries.R")
source("R/0_functions.R")

future::plan("multisession")

ano <- 2018:2020

months <- c(paste0("0", 1:9), 10:12)

estados <- readr::read_csv("data/aux_files/estados.csv") |> purrr::flatten_chr()

bases <- c("DET", "CONS")

parametros <- list(
  estados = estados,
  faixas = readr::read_csv("data/aux_files/faixas.csv") |> purrr::flatten_chr(),
  sexos = readr::read_csv("data/aux_files/sexos.csv") |> purrr::flatten_chr()
)

urls_base <- c(
  hosp = "http://ftp.dadosabertos.ans.gov.br/FTP/PDA/TISS/HOSPITALAR/",
  amb = "http://ftp.dadosabertos.ans.gov.br/FTP/PDA/TISS/AMBULATORIAL/"
)

termos <- arrow::read_parquet("data/tabelas_tuss.parquet") |>
  dplyr::pull(cd_procedimento)

# função de download ------------------------------------------------------

for (j in c("hosp", "amb")) {
  purrr::walk(
    ano,
    function(ano) {
      # definindo termos da buscas no dados abertos -----------------------

      cat(glue::glue("\n========== BASE: {stringr::str_to_upper(j)}, ANO: {ano} ==========\n"))

      urls <- purrr::map(
        bases,
        ~ base(
          ano = ano,
          estado = estados,
          mes = months,
          base = .x,
          url_base = glue::glue("{urls_base[j]}"),
          proc = stringr::str_to_upper(j)
        )
      )

      # download e escrita em database ------------------------------------

      fs::dir_create("data/parquet/")

      cat("\n===== DOWNLOAD ====\n")

      pbapply::pblapply(
        seq_len(nrow(urls[[1]])),
        function(i) {
          unpack_write_parquet(
            url = urls[[1]]$url[i],
            date = urls[[1]]$date[i],
            cols = c(
              "id_evento_atencao_saude",
              "cd_procedimento",
              "cd_tabela_referencia",
              "uf_prestador",
              "qt_item_evento_informado",
              "vl_item_evento_informado",
              "ind_tabela_propria"
            )
          )

          gc()
        },
        cl = parallel::detectCores()
      )

      pbapply::pblapply(
        seq_len(nrow(urls[[2]])),
        function(i) {
          unpack_write_parquet(
            url = urls[[2]]$url[i],
            date = urls[[2]]$date[i],
            cols = c(
              "id_evento_atencao_saude",
              "faixa_etaria",
              "sexo"
            )
          )

          gc()
        },
        cl = parallel::detectCores()
      )

      # merge entre bases DET e CONS --------------------------------------

      cat("\n===== MERGE ====\n")

      fs::dir_create(glue::glue("data/proc_{j}_db/"))

      det_db <- fs::dir_ls(path = "data/parquet/", regexp = "*DET.parquet")

      cons_db <- fs::dir_ls(path = "data/parquet/", regexp = "*CONS.parquet")

      pbapply::pblapply(
        seq_len(length(det_db)),
        function(i) {
          merge_db(
            path_1 = det_db[i],
            path_2 = cons_db[i],
            termos = termos
          )

          gc()
        },
        cl = parallel::detectCores()
      )

      fs::dir_delete("data/parquet/")

      # tratando database -------------------------------------------------

      cat("\n===== EXPORT ====\n")

      estatisticas <- list(
        cols = c("uf_prestador", "faixa_etaria", "sexo"),
        names = glue::glue("base_{j}_{c('uf_prestador', 'idade', 'sexo')}_{ano}")
      )

      fs::dir_create("output/export")

      purrr::walk(
        1:3,
        ~ export_parquet(
          x = estatisticas$cols[.x],
          complete_vars = parametros[[.x]],
          export_name = estatisticas$names[.x],
          db_name = glue::glue("proc_{j}_db"),
          months = months
        )
      )

      db <- arrow::open_dataset("output/export") |>
        dplyr::compute()

      arrow::write_parquet(db, glue::glue("output/base_{j}_{ano}.parquet"))

      gc()

      purrr::walk(
        c("output/export/", glue::glue("data/proc_{j}_db/")),
        fs::dir_delete
      )
    }
  )
}
