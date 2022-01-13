#### BASE CONSOLIDADA DE PROCEDIMENTOS AMBULATORIAIS POR UF ####

# bibliotecas e funções ---------------------------------------------------

source("R/bibliotecas.R")
source("R/funcoes.R")
source("R/dic_tuss_db.R")

# definindo termos da buscas no dados abertos -----------------------------

bases <- c("DET", "CONS")

estados <- c("AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "TO", "SP")

urls <- purrr::map(
  bases,
  ~ purrr::map2(
    .x, estados,
    ~ base(
      ano = "2020",
      base = .x,
      estado = .y,
      mes = c("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"),
      url = "http://ftp.dadosabertos.ans.gov.br/FTP/PDA/TISS/AMBULATORIAL/",
      proc = "AMB"
    )
  )
)

# memory.size(max = 10^12)

future::plan(multisession) # habilitando multithread

# tratando procedimentos ambulatoriais um estado por vez ------------------

amb <- function(urls, tabelas, index) {

  ## download e leitura de dados ----

  base_det <- furrr::future_map_dfr(
    urls[[1]][[index]],
    ~ unpack_read(
      .x,
      c(
        "ID_EVENTO_ATENCAO_SAUDE",
        "UF_PRESTADOR",
        "CD_PROCEDIMENTO",
        "CD_TABELA_REFERENCIA",
        "QT_ITEM_EVENTO_INFORMADO",
        "VL_ITEM_EVENTO_INFORMADO"
      )
    )
  ) |>
    janitor::clean_names()

  base_cons <- furrr::future_map_dfr(
    urls[[2]][[index]],
    ~ unpack_read(
      .x,
      c(
        "ID_EVENTO_ATENCAO_SAUDE",
        "FAIXA_ETARIA",
        "SEXO"
      )
    )
  ) |>
    janitor::clean_names()

  base_det <- base_det[, collapse::na_omit(base_det)]

  base_cons <- base_cons[, collapse::na_omit(base_cons)]

  base_amb <- data.table::merge.data.table(
    base_det,
    base_cons,
    on = "id_evento_atencao_saude"
  )

  base_amb <- base_amb[
    cd_tabela_referencia != 0 &
      cd_tabela_referencia != 9 &
      cd_tabela_referencia != 98,
    !"cd_tabela_referencia"
  ]

  ## join por dicionário ----

  base_amb[
    ,
    cd_procedimento := as.numeric(cd_procedimento)
  ]

  base_amb <- data.table::merge.data.table(
    base_amb,
    tabelas,
    by = "cd_procedimento",
    allow.cartesian = TRUE
  )

  base_amb[
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

  ## lista de procedimentos disponíveis ----

  base_amb[
    ,
    collapse::funique(.SD, cols = "termo")
  ][
    ,
    "termo"
  ][
    ,
    termo := furrr::future_map_chr(
      termo,
      stringr::str_to_upper
    )
  ] |>
    data.table::fwrite(
      "output/termos_amb.csv",
      append = TRUE)

  ## estatísticas por estado ----

  base_amb_uf <- base_amb[
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
    future.apply::future_lapply(
      .SD,
      collapse::replace_NA,
      value = 0
    )
  ]

  data.table::setnames(
    base_amb_uf,
    old = "uf_prestador",
    new = "categoria"
  )

  data.table::fwrite(
    base_amb_uf,
    "output/base_amb_uf.csv",
    append = TRUE
  )

  ## estatísticas por faixa etária ----

  base_amb_idade <- base_amb[
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
    future.apply::future_lapply(
      .SD,
      collapse::replace_NA,
      value = 0
    )
  ]

  data.table::setnames(
    base_amb_idade,
    old = "faixa_etaria",
    new = "categoria"
  )

  data.table::fwrite(
    base_amb_idade,
    "output/base_amb_idade.csv",
    append = TRUE
  )

  ## estatísticas por sexo ----

  base_amb_sexo <- base_amb[
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
    future.apply::future_lapply(
      .SD,
      collapse::replace_NA,
      value = 0
    )
  ]

  data.table::setnames(
    base_amb_sexo,
    old = "sexo",
    new = "categoria"
  )

  data.table::fwrite(
    base_amb_sexo,
    "output/base_amb_sexo.csv",
    append = TRUE
  )
}

# exportando dados --------------------------------------------------------

# rodar loop de 5 em 5

for (i in 1:26) {
  furrr::future_walk(
    i,
    ~ amb(urls, tabelas, .x)
  )

  gc()
}

# limpando exportação -----------------------------------------------------

fix_db <- function(path, factors) {
  df <- readr::read_csv(path) |>
    dplyr::distinct(cd_procedimento, termo, categoria) |>
    data.table::as.data.table()

  df <- df[
    ,
    .SD[.(categoria = factors),
      on = "categoria"
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

  data.table::fwrite(
    df,
    path
  )
}

files <- as.character(fs::dir_ls(path = "output/", regexp = "._amb_."))

factors <- list(
  idade = c("1 a 4", "10 a 14", "15 a 19", "20 a 29", "30 a 39", "40 a 49", "5 a 9", "50 a 59", "60 a 69", "70 a 79", "80 <", "< 1", "N. I."),
  sexo = c("Masculino", "Feminino", "N. I."),
  uf = c("AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO")
)

parametros <- purrr::map2_dfr(
  files, factors,
  ~ tibble::tibble(
    path = .x,
    factors = list(.y)
  )
)

purrr::walk(
  1:3,
  ~ fix_db(parametros$path[.x], parametros$factors[[.x]])
)

readr::read_csv("output/termos_amb.csv") |>
  dplyr::distinct(termo) |>
  readr::write_csv("output/termos_amb.csv")

