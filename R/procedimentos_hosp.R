#### BASE CONSOLIDADA DE PROCEDIMENTOS HOSPITALARES POR UF ####

# bibliotecas e funções ---------------------------------------------------

rm(list = ls())

source("R/bibliotecas.R")
source("R/funcoes.R")
source("R/dic_tuss_db.R")

# definindo termos da buscas no dados abertos -----------------------------

# base DET (2020)

url <- base(
  ano = "2020",
  estado = c("AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"), # pesquisa múltipla
  mes = c("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"), # pesquisa múltipla
  base = "DET"
)

# base CONS (2020)

url2 <- base(
  ano = "2020",
  estado = c("AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"), # pesquisa múltipla
  mes = c("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"), # pesquisa múltipla
  base = "CONS"
) # iterar para múltiplos períodos e base

# download e leitura de dados ---------------------------------------------

memory.size(max = 10^12)

future::plan(multisession) # habilitando multithread

base_det <- furrr::future_map_dfr(
  url,
  ~ unpack_read(
    .x,
    c(
      "CD_PROCEDIMENTO",
      "ID_EVENTO_ATENCAO_SAUDE",
      "UF_PRESTADOR",
      "CD_TABELA_REFERENCIA",
      "QT_ITEM_EVENTO_INFORMADO",
      "VL_ITEM_EVENTO_INFORMADO"
    )
  )
) |> # função de importação multithread
  janitor::clean_names()

base_det <- base_det[, collapse::na_omit(base_det)]

base_cons <- furrr::future_map_dfr(
  url2,
  ~ unpack_read(
    .x,
    c(
      "ID_EVENTO_ATENCAO_SAUDE",
      "FAIXA_ETARIA",
      "SEXO",
      "TEMPO_DE_PERMANENCIA",
      "CD_CARATER_ATENDIMENTO"
    )
  )
) |> # função de importação multithread
  janitor::clean_names()

base_cons <- base_cons[, collapse::na_omit(base_cons)]

# join

base_hosp <- data.table::merge.data.table(base_det,
  base_cons,
  on = "id_evento_atencao_saude"
)

base_hosp <- base_hosp[
  cd_tabela_referencia != 0 &
    cd_tabela_referencia != 9 &
    cd_tabela_referencia != 98,
  !"cd_tabela_referencia"
]

# join por dicionário -----------------------------------------------------

base_hosp[
  ,
  cd_procedimento := as.numeric(cd_procedimento)
]

base_hosp <- data.table::merge.data.table(
  base_hosp,
  tabelas,
  by = "cd_procedimento",
  allow.cartesian = TRUE
)

base_hosp[
  ,
  ":="(termo = collapse::replace_NA(
    termo,
    "Sem informações"
  ),
  tabela = collapse::replace_NA(
    tabela,
    "Sem informações"
  ))
]

# lista de procedimentos disponíveis --------------------------------------

base_hosp |>
  dplyr::distinct(termo) |>
  dplyr::mutate(
    termo = purrr::map_chr(
      termo,
      stringr::str_to_upper
    )
  ) |>
  readr::write_csv("output/termos.csv")

# estatísticas por estado -------------------------------------------------

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
  termo := future.apply::future_lapply(
    termo,
    stringr::str_to_upper
  )
]

data.table::setnames(
  base_hosp_uf, 
  old = "uf_prestador", 
  new = "categoria")

data.table::fwrite(
  base_hosp_uf,
  "output/base_hosp_uf.csv"
)

# estatísticas por faixa etária -------------------------------------------

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
  termo := future.apply::future_lapply(
    termo,
    stringr::str_to_upper
  )
]

data.table::setnames(
  base_hosp_idade, 
  old = "faixa_etaria", 
  new = "categoria")

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
]

data.table::setnames(
  base_hosp_sexo, 
  old = "sexo", 
  new = "categoria")

data.table::fwrite(
  base_hosp_sexo,
  "output/base_hosp_sexo.csv"
)