#### BASE CONSOLIDADA DE PROCEDIMENTOS HOSPITALARES POR UF ####

# bibliotecas e funções ---------------------------------------------------

source("R/libraries.R")
source("R/functions.R")
# source("R/tabelas_tuss.R")

# definindo termos da buscas no dados abertos -----------------------------

bases <- c("DET", "CONS")

urls <- purrr::map(
  bases,
  ~ base(
    ano = "2020",
    estado = c("AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"),
    mes = c("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"),
    base = .x,
    url = "http://ftp.dadosabertos.ans.gov.br/FTP/PDA/TISS/HOSPITALAR/",
    proc = "HOSP"
  )
)

# criação da database ------------------------------------------------

## download e escrita em database ----

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
  dbdir = "input/proc_hosp_amb.duckdb"
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
  statement = "EXPORT DATABASE 'input/db' (FORMAT CSV)"
)

DBI::dbDisconnect(con, shutdown = TRUE)

fs::file_delete("input/proc_hosp_amb.duckdb")

## criando nova database limpa ----

con <- duckdb::dbConnect(
  duckdb::duckdb(),
  dbdir = "input/proc_hosp_amb.duckdb"
)

DBI::dbExecute(
  conn = con,
  statement = "IMPORT DATABASE 'input/db'")

indexes <- glue::glue_sql(
  "CREATE INDEX idx
  ON proc (id_evento_atencao_saude, cd_procedimento, faixa_etaria, sexo, uf_prestador)",
  .con = con
)

DBI::dbExecute(conn = con, statement = indexes)

fs::file_delete("input/db")

# DBI::dbDisconnect(con, shutdown = TRUE)

# database do shinyapp ----------------------------------------------------

shinydb <- duckdb::dbConnect(
  duckdb::duckdb(),
  dbdir = "output/shinydb.duckdb"
)

df <- dplyr::tbl(con, "proc")

## lista de procedimentos disponíveis ----

termos_hosp <- df |>
  dplyr::select(termo) |>
  dplyr::collect() |>
  dplyr::distinct() |>
  dplyr::mutate(
    termo = purrr::map_chr(
      termo,
      stringr::str_to_upper
    )
  )

dplyr::copy_to(
  dest = shinydb,
  df = termos_hosp,
  name = "termos_hosp",
  indexes = "termo"
)

## estatísticas por estado ----

df |>
  dplyr::group_by(cd_procedimento, termo, uf_prestador) |>
  dplyr::mutate(
    tot_qt = sum(qt_item_evento_informado, na.rm = TRUE),
    tot_vl = sum(vl_item_evento_informado, na.rm = TRUE)
  )

base_hosp_uf <- base_hosp[
  ,
  .(
    tot_qt = collapse::fsum(qt_item_evento_informado),
    tot_vl = collapse::fsum(vl_item_evento_informado)
  ),
  keyby = c("cd_procedimento", "termo", "uf_prestador")
][
  ,
  mean_vl := round(tot_vl / tot_qt, 2),
  keyby = c("cd_procedimento", "termo", "uf_prestador")
][
  ,
  termo := furrr::future_map_chr(
    termo,
    stringr::str_to_upper
  )
][
  termo != "SEM INFORMAÇÕES"
][
  ,
  .SD[.(uf_prestador = estados),
    on = "uf_prestador"
  ],
  by = .(cd_procedimento, termo) # completando UF's faltantes
][
  ,
  future.apply::future_lapply(
    .SD,
    collapse::replace_NA,
    value = 0
  )
]

data.table::setnames(
  base_hosp_uf,
  old = "uf_prestador",
  new = "categoria"
)

data.table::fwrite(
  base_hosp_uf,
  "output/base_hosp_uf.csv"
)

# estatísticas por faixa etária -------------------------------------------

faixas <- c("1 a 4", "10 a 14", "15 a 19", "20 a 29", "30 a 39", "40 a 49", "5 a 9", "50 a 59", "60 a 69", "70 a 79", "80 <", "< 1", "N. I.")

base_hosp_idade <- base_hosp[
  ,
  .(
    tot_qt = collapse::fsum(qt_item_evento_informado),
    tot_vl = collapse::fsum(vl_item_evento_informado)
  ),
  keyby = c("cd_procedimento", "termo", "faixa_etaria")
][
  ,
  mean_vl := round(tot_vl / tot_qt, 2),
  keyby = c("cd_procedimento", "termo", "faixa_etaria")
][
  ,
  termo := furrr::future_map_chr(
    termo,
    stringr::str_to_upper
  )
][
  ,
  faixa_etaria := tidyfast::dt_case_when(
    faixa_etaria == "<1" ~ "< 1",
    faixa_etaria == "80 ou mais" ~ "80 <",
    TRUE ~ faixa_etaria
  )
][
  ,
  .SD[.(faixa_etaria = faixas),
    on = "faixa_etaria"
  ],
  by = .(cd_procedimento, termo) # completando faixas etárias faltantes
][
  ,
  future.apply::future_lapply(
    .SD,
    collapse::replace_NA,
    value = 0
  )
]

data.table::setnames(
  base_hosp_idade,
  old = "faixa_etaria",
  new = "categoria"
)

data.table::fwrite(
  base_hosp_idade,
  "output/base_hosp_idade.csv"
)

# estatísticas por sexo ---------------------------------------------------

base_hosp_sexo <- base_hosp[
  ,
  .(
    tot_qt = collapse::fsum(qt_item_evento_informado),
    tot_vl = collapse::fsum(vl_item_evento_informado)
  ),
  keyby = c("cd_procedimento", "termo", "sexo")
][
  ,
  mean_vl := round(tot_vl / tot_qt, 2),
  keyby = c("cd_procedimento", "termo", "sexo")
][
  ,
  sexo := tidyfast::dt_case_when(
    sexo %not_in% c("Masculino", "Feminino") ~ "N. I.",
    TRUE ~ sexo
  )
][
  ,
  .SD[.(sexo = c("Masculino", "Feminino", "N. I.")),
    on = "sexo"
  ],
  by = .(cd_procedimento, termo) # completando sexos faltantes
][
  ,
  future.apply::future_lapply(
    .SD,
    collapse::replace_NA,
    value = 0
  )
]

data.table::setnames(
  base_hosp_sexo,
  old = "sexo",
  new = "categoria"
)

data.table::fwrite(
  base_hosp_sexo,
  "output/base_hosp_sexo.csv"
)
