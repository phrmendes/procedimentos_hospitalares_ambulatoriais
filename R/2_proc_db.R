# -------------------------------------------------------------- #
# --- DATABASE DE PROCEDIMENTOS HOSPITALARES E AMBULATORIAIS --- #
# -------------------------------------------------------------- #

# bibliotecas, funções, parâmetros e variáveis ----------------------------

source("R/0_libraries.R")
source("R/0_functions.R")

future::plan("multisession")

years <- 2018:2020

months <- c(paste0("0", 1:9), 10:12)

ufs <- readr::read_csv(
  "data/aux_files/estados.csv",
  show_col_types = FALSE
) |>
  dplyr::select(estados) |>
  purrr::flatten_chr()

types_db <- c("DET", "CONS")

ftp_urls <- c(
  hosp = "http://ftp.dadosabertos.ans.gov.br/FTP/PDA/TISS/HOSPITALAR/",
  amb = "http://ftp.dadosabertos.ans.gov.br/FTP/PDA/TISS/AMBULATORIAL/"
)

tuss <- arrow::read_parquet("data/tabelas_tuss.parquet") |>
  dtplyr::lazy_dt(key_by = "cd_procedimento")

vec <- c("hosp", "amb")

# download e tratamento de dados ------------------------------------------

for (j in vec) {
  purrr::walk(
    years,
    function(i) {
      # definindo termos da buscas no dados abertos -----------------------

      cat(glue::glue("\n========== DB: {stringr::str_to_upper(j)} - YEAR: {i} ==========\n"))

      urls <- purrr::map(
        types_db,
        ~ urls_ftp_ans(
          year = i,
          uf = ufs,
          months = months,
          base = .x,
          ftp_url = glue::glue("{ftp_urls[j]}"),
          proc = stringr::str_to_upper(j)
        )
      )

      # download e escrita em database ------------------------------------

      fs::dir_create("data/parquet/")

      cat("\n===== DOWNLOAD =====\n")

      pbapply::pblapply(
        seq_len(nrow(urls[[1]])),
        function(i) {
          unpack_write(
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

      cat("\n===== MERGE =====\n")

      fs::dir_create(glue::glue("data/{j}_db/"))

      det_db <- fs::dir_ls(path = "data/parquet/", regexp = "*DET.parquet")

      cons_db <- fs::dir_ls(path = "data/parquet/", regexp = "*CONS.parquet")

      pbapply::pblapply(
        seq_len(length(det_db)),
        function(k) {
          merge_parquets(
            path_1 = det_db[k],
            path_2 = cons_db[k]
          )

          gc()
        },
        cl = parallel::detectCores()
      )

      fs::dir_delete("data/parquet/")

      # tratando database -------------------------------------------------

      cat("\n===== EXPORT =====\n")

      par <- list(
        cols = c("uf_prestador", "faixa_etaria", "sexo"),
        names = glue::glue("{j}_{c('uf', 'idade', 'sexo')}_{i}")
      )

      fs::dir_create("output/export")

      purrr::walk(
        1:3,
        ~ export_parquets(
          x = par$cols[.x],
          export_name = par$names[.x],
          db_name = glue::glue("{j}_db"),
          months = months
        )
      )

      db <- arrow::open_dataset("output/export") |>
        dplyr::compute()

      arrow::write_parquet(
        db,
        glue::glue("output/{j}_{i}.parquet")
      )

      gc()

      purrr::walk(
        c("output/export/", glue::glue("data/{j}_db/")),
        fs::dir_delete
      )
    }
  )
}

# arrow datasets ----------------------------------------------------------

db <- list(
  base_hosp = fs::dir_ls("output/", regexp = "hosp(.*)\\.parquet") |>
    purrr::map_dfr(arrow::read_parquet),
  base_amb = fs::dir_ls("output/", regexp = "amb(.*)\\.parquet") |>
    purrr::map_dfr(arrow::read_parquet)
)

db <- purrr::map2(
  .x = db,
  .y = c("hosp", "amb"),
  ~ .x[
    ,
    db := .y
  ][
    ,
    ":="(
      ano = as.integer(ano),
      mes = as.integer(mes)
    )
  ]
) |>
  data.table::rbindlist() |>
  dtplyr::lazy_dt(key_by = "cd_procedimento")

db |>
  dplyr::left_join(
    tuss,
    by = "cd_procedimento"
  ) |>
  dplyr::filter(!is.na(termo) | termo != "sem_info") |>
  dplyr::select(cd_procedimento, termo, ano, db) |>
  dplyr::distinct() |>
  data.table::as.data.table() |>
  arrow::write_dataset(
    "output/db_termos_shiny",
    format = "parquet",
    partitioning = c("db", "ano")
  )

arrow::write_dataset(
  data.table::as.data.table(db),
  "output/db_shiny",
  format = "parquet",
  partitioning = c("db", "tipo")
)

zip::zip(
  zipfile = fs::file_create("output/bkp_db.zip"),
  files = fs::dir_ls("output/", regexp = ".parquet")
)

fs::dir_ls("output/", regexp = "(hosp|amb)(.*)\\.parquet") |>
  fs::file_delete()
