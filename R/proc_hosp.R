#### BASE CONSOLIDADA DE PROCEDIMENTOS HOSPITALARES POR UF ####

# bibliotecas e funções ---------------------------------------------------

source("R/libraries.R")
source("R/functions.R")
source("R/tabelas_tuss.R")

# definindo termos da buscas no dados abertos -----------------------------

estados <- c("AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO")

bases <- c("DET", "CONS")

urls <- purrr::map(
  bases,
  ~ base(
    ano = "2020",
    estado = estados,
    mes = c("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"),
    base = .x,
    url = "http://ftp.dadosabertos.ans.gov.br/FTP/PDA/TISS/HOSPITALAR/",
    proc = "HOSP"
  )
)

# criação da database ------------------------------------------------

## download e escrita em database ----

future::plan(multisession) # habilitando multithread

pbapply::pblapply(
  urls[[1]],
  function(i) {
    unpack_write_db(
      url = i,
      cols = c(
        "cd_procedimento",
        "id_evento_atencao_saude",
        "uf_prestador",
        "cd_tabela_referencia",
        "qt_item_evento_informado",
        "vl_item_evento_informado"
      ),
      name = "base_det",
      indexes = list(
        "cd_procedimento",
        "id_evento_atencao_saude",
        "uf_prestador"
      )
    )
  }
)

pbapply::pblapply(
  urls[[2]],
  function(i) {
    unpack_write_db(
      url = i,
      cols = c(
        "id_evento_atencao_saude",
        "faixa_etaria",
        "sexo",
        "cd_carater_atendimento"
      ),
      name = "base_cons",
      indexes = list(
        "id_evento_atencao_saude",
        "sexo",
        "faixa_etaria",
      )
    )
  }
)

## join ----

# usar linux para realizar o join

con <- duckdb::dbConnect(
  duckdb::duckdb(),
  dbdir = "input/proc.duckdb"
)

subquery <- glue::glue_sql(
  "SELECT *
  FROM base_cons AS cons
  JOIN base_det AS det
  ON cons.id_evento_atencao_saude = det.id_evento_atencao_saude",
  .con = con
)

join <- glue::glue_sql(
  "SELECT *
  FROM ({subquery}) AS a
  LEFT JOIN tabelas_tuss AS b
  ON a.cd_procedimento = b.cd_procedimento",
  .con = con
)

create_table <- glue::glue_sql(
  "CREATE TABLE proc
  AS ({join})",
  .con = con
)

DBI::dbExecute(conn = con, statement = create_table)

## limpando e exportando database ----

purrr::walk(
  c("base_cons", "base_det", "tabelas_tuss"),
  ~ duckdb::dbRemoveTable(con, .x)
)

purrr::walk(
  c("cd_procedimento:1", "id_evento_atencao_saude:1"),
  ~ DBI::dbExecute(
    conn = con,
    statement = glue::glue(
      'ALTER TABLE proc
      DROP COLUMN IF EXISTS "{paste0(.x)}"'
    )
  )
)

DBI::dbExecute(
  conn = con,
  statement = "COPY proc TO 'input/proc.csv'"
)

duckdb::dbDisconnect(con, shutdown = TRUE)

fs::file_delete("input/proc.duckdb")

## criando nova database limpa ----

con <- duckdb::dbConnect(
  duckdb::duckdb(),
  dbdir = "input/proc_hosp_amb.duckdb"
)

DBI::dbExecute(
  conn = con,
  statement = "CREATE SCHEMA pg_catalog")

DBI::dbExecute(
  conn = con,
  statement = "CREATE TABLE proc_hosp(id_evento_atencao_saude VARCHAR, faixa_etaria VARCHAR, sexo VARCHAR, cd_carater_atendimento INTEGER, cd_procedimento VARCHAR, uf_prestador VARCHAR, cd_tabela_referencia INTEGER, qt_item_evento_informado INTEGER, vl_item_evento_informado DOUBLE, termo VARCHAR, tabela VARCHAR)")

DBI::dbExecute(
  conn = con,
  statement = "COPY proc FROM 'input/proc.csv' (HEADER 0)")

DBI::dbExecute(
  conn = con,
  statement = "CREATE INDEX idx ON proc_hosp (id_evento_atencao_saude, cd_procedimento, faixa_etaria, sexo, uf_prestador)")

fs::file_delete("input/proc.csv")

gc()

# database do shinyapp ----------------------------------------------------

## lista de procedimentos disponíveis

shinydb <- duckdb::dbConnect(
  duckdb::duckdb(),
  dbdir = "output/shinydb.duckdb"
)

termos_hosp <- tbl(con, "proc_hosp") |>
  dplyr::select(termo) |>
  dplyr::distinct() |>
  dplyr::collect() |>
  data.table::as.data.table()

termos_hosp[
  ,
  termo := furrr::future_map_chr(
    termo,
    stringr::str_to_upper
  )
]

duckdb::dbWriteTable(
  conn = shinydb,
  value = termos_hosp,
  name = "termos_hosp",
  overwrite = TRUE,
  temporary = FALSE
)

purrr::walk(
  c(con, shinydb),
  ~ duckdb::dbDisconnect(.x, shutdown = TRUE)
)

## shapefile de estados

geobr::read_state(
  code_state = "all",
  showProgress = FALSE
) |>
  dplyr::select(code_state, abbrev_state, geom) |>
  dplyr::rename(categoria = abbrev_state) |>
  saveRDS(file = "output/geom_ufs.rds")

## estatísticas por estado

import_shinydb(
  x = "uf_prestador",
  complete_vars = estados,
  db_name = "base_hosp_uf",
  table = "proc_hosp"
)

## estatísticas por faixa etária

faixas <- c("1 a 4", "10 a 14", "15 a 19", "20 a 29", "30 a 39", "40 a 49", "5 a 9", "50 a 59", "60 a 69", "70 a 79", "80 <", "< 1", "N. I.")

import_shinydb(
  x = "faixa_etaria",
  complete_vars = faixas,
  db_name = "base_hosp_idade",
  table = "proc_hosp"
)

## estatísticas por sexo

import_shinydb(
  x = "sexo",
  complete_vars = c("Masculino", "Feminino", "N. I."),
  db_name = "base_hosp_sexo",
  table = "proc_hosp"
)
