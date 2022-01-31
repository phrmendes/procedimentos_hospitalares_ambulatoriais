########################################################
#### BASE CONSOLIDADA DE PROCEDIMENTOS HOSPITALARES ####
########################################################

# bibliotecas, funções e parâmetros ---------------------------------------

source("R/1_libraries.R")
source("R/2_functions.R")

future::plan(multisession) # habilitando multithread

# memory.size(max = 10^12)

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

# download e escrita em database ------------------------------------------

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


# tratando database -------------------------------------------------------

# conexão com database intermediária

con <- duckdb::dbConnect(
  duckdb::duckdb(),
  dbdir = "data/proc.duckdb"
)

# passando tabelas tuss para a database atual

tabs <- duckdb::dbConnect(
  duckdb::duckdb(),
  dbdir = "data/tabelas_tuss.duckdb"
)

tuss <- dplyr::tbl(tabs, "tabelas_tuss") |>
  dplyr::collect()

duckdb::dbWriteTable(
  conn = con,
  value = tuss,
  name = "tabelas_tuss",
  overwrite = TRUE,
  temporary = FALSE
)

duckdb::dbDisconnect(tabs, shutdown = TRUE)

# joins

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

# limpando e exportando database

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
  statement = "COPY proc TO 'data/proc.csv'"
)

duckdb::dbDisconnect(con, shutdown = TRUE)

fs::file_delete("data/proc.duckdb")

# criando nova database limpa

con <- duckdb::dbConnect(
  duckdb::duckdb(),
  dbdir = "data/proc_hosp.duckdb"
)

DBI::dbExecute(
  conn = con,
  statement = "CREATE SCHEMA pg_catalog")

DBI::dbExecute(
  conn = con,
  statement = "CREATE TABLE proc_hosp(id_evento_atencao_saude VARCHAR, faixa_etaria VARCHAR, sexo VARCHAR, cd_carater_atendimento INTEGER, cd_procedimento VARCHAR, uf_prestador VARCHAR, cd_tabela_referencia INTEGER, qt_item_evento_informado INTEGER, vl_item_evento_informado DOUBLE, termo VARCHAR, tabela VARCHAR)")

DBI::dbExecute(
  conn = con,
  statement = "COPY proc FROM 'data/proc.csv' (HEADER 0)")

DBI::dbExecute(
  conn = con,
  statement = "CREATE INDEX idx ON proc_hosp (id_evento_atencao_saude, cd_procedimento, faixa_etaria, sexo, uf_prestador)")

fs::file_delete("data/proc.csv")

# desligando base

duckdb::dbDisconnect(con, shutdown = TRUE)

