########################################################
#### BASE CONSOLIDADA DE PROCEDIMENTOS HOSPITALARES ####
########################################################

# bibliotecas, funções e parâmetros ---------------------------------------

source("R/1_libraries.R")
source("R/2_functions.R")

# memory.size(max = 10^12)

# dicionário de termos ----------------------------------------------------

tabs <- duckdb::dbConnect(
  duckdb::duckdb(),
  dbdir = "data/tabelas_tuss.duckdb"
)

tuss <- dplyr::tbl(tabs, "tabelas_tuss") |>
  dplyr::collect()

duckdb::dbDisconnect(tabs, shutdown = TRUE)

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

pbapply::pbmapply(
  function(x, y, z) {
    unpack_write_db(
      url = x,
      mes = y,
      dic = tuss,
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
        "id_evento_atencao_saude",
        "mes",
        "cd_procedimento",
        "uf_prestador"
      )
    )
  },
  x = urls[[1]]$url,
  y = urls[[1]]$mes,
  z = tuss,
  SIMPLIFY = FALSE
)

pbapply::pbmapply(
  function(x, y, z) {
    unpack_write_db(
      url = x,
      mes = y,
      dic = tuss,
      cols = c(
        "id_evento_atencao_saude",
        "faixa_etaria",
        "sexo"
      ),
      name = "base_cons",
      indexes = list(
        "id_evento_atencao_saude",
        "sexo",
        "faixa_etaria",
      )
    )
  },
  x = urls[[2]]$url,
  y = urls[[2]]$mes,
  z = tuss,
  SIMPLIFY = FALSE
)

# tratando database -------------------------------------------------------

# conexão com database intermediária

con <- duckdb::dbConnect(
  duckdb::duckdb(),
  dbdir = "data/proc.duckdb"
)

# passando tabelas tuss para a database atual

duckdb::dbWriteTable(
  conn = con,
  value = tuss,
  name = "tabelas_tuss",
  overwrite = TRUE,
  temporary = FALSE
)

# joins

subquery <- glue::glue_sql(
  "SELECT *
  FROM base_det AS det
  LEFT JOIN base_cons AS cons
  ON det.id_evento_atencao_saude = cons.id_evento_atencao_saude",
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
  c("cd_procedimento:1", "id_evento_atencao_saude:1", "mes:1"),
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
  statement = "COPY proc TO 'data/proc.csv' WITH (HEADER 1)"
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

duckdb::duckdb_read_csv(
  conn = con,
  name = "proc_hosp",
  files = "data/proc.csv",
  header = TRUE
)

DBI::dbExecute(
  conn = con,
  statement = "CREATE INDEX idx ON proc_hosp (id_evento_atencao_saude, cd_procedimento, faixa_etaria, sexo, uf_prestador)")

fs::file_delete("data/proc.csv")

# desligando base

duckdb::dbDisconnect(con, shutdown = TRUE)
