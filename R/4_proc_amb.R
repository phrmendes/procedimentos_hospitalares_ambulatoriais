#########################################################
#### BASE CONSOLIDADA DE PROCEDIMENTOS AMBULATORIAIS ####
#########################################################

#' Esse código deve ser executado em uma VM do gcloud a partir do
#' pacote "googleComputeEngineR" em uma máquina "n1-standard-8". O
#' loop de exportação foi executado de 5 em 5 estados.

# bibliotecas -------------------------------------------------------------

if (!require("pacman")) install.packages("pacman")

pacman::p_load(
  data.table,
  collapse,
  tidyverse,
  glue,
  tidyfast,
  future,
  furrr,
  pbapply,
  janitor,
  fs,
  duckdb,
  install = F
)

# funções -----------------------------------------------------------------

# função para seleção de bases por ano, estado, mês e período

base <- function(ano, estado, mes, base, url, proc) {
  x <- tibble::tibble(
    a = glue::glue("{url}{ano}/"),
    b = estado,
    c = glue::glue("_{ano}"),
  ) |> # criando tibble base para operação
    dplyr::group_by(a, b, c) |>
    tidyr::nest() |> # criando subtibbles para cada região
    dplyr::mutate(
      d = glue::glue("/{b}"), # adicionando coluna auxiliar para união de url
      data = purrr::map(
        data,
        ~ tibble::tibble(
          e = mes, # criando coluna de meses em cada subtiblle
          f = glue::glue("_{proc}_{base}.zip"),
          mes = mes,
        )
      )
    ) |>
    tidyr::unnest(cols = c(data)) |>
    tidyr::unite("url",
                 c(a, b, d, c, e, f),
                 sep = ""
    )

  return(x)
}

# função de descompactação e leitura

unpack_read <- function(url, mes, cols) {
  temp <- tempfile()

  tempdir <- tempdir()

  download.file(
    url = url,
    destfile = temp,
    method = "auto",
    quiet = T
  )

  csv_file <- unzip(
    zipfile = temp,
    exdir = tempdir
  )

  x <- data.table::fread(
    input = csv_file,
    encoding = "UTF-8",
    select = stringr::str_to_upper(cols),
    sep = ";",
    dec = ","
  ) |> janitor::clean_names()

  x <- x[
    ,
    collapse::na_omit(x)
  ][
    ,
    mes := mes
  ]

  purrr::walk(
    c(temp, csv_file),
    ~ fs::file_delete(glue::glue("{.x}"))
  )

  gc()

  return(x)
}

`%not_in%` <- Negate(`%in%`)

# parâmetros --------------------------------------------------------------

# future::plan(multisession) # habilitando multithread

# memory.size(max = 10^12)

future::plan(multicore) # habilitando multithread

# definindo termos da buscas no dados abertos -----------------------------

estados <- c("AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "TO", "SP")

bases <- c("DET", "CONS")

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
) # dividindo urls por base e por estado

# dicionário de termos ----------------------------------------------------

con <- duckdb::dbConnect(
  duckdb::duckdb(),
  dbdir = "data/tabelas_tuss.duckdb"
)

tabelas <- tbl(con, "tabelas_tuss") |>
  dplyr::collect()

duckdb::dbDisconnect(con, shutdown = TRUE)

# função de tratamento de dados ambulatoriais -----------------------------

amb <- function(urls, index, tabelas) {

  # download e leitura de dados ----

  base_det <- furrr::future_map2_dfr(
    urls[[1]][[index]]$url, urls[[1]][[index]]$mes,
    ~ unpack_read(
      url = .x,
      mes = .y,
      cols = c(
        "cd_procedimento",
        "id_evento_atencao_saude",
        "uf_prestador",
        "cd_tabela_referencia",
        "qt_item_evento_informado",
        "vl_item_evento_informado"
      )
    )
  ) |> collapse::funique(cols = c("cd_procedimento", "id_evento_atencao_saude", "mes"))

  base_cons <- furrr::future_map2_dfr(
    urls[[2]][[index]]$url, urls[[2]][[index]]$mes,
    ~ unpack_read(
      url = .x,
      mes = .y,
      cols = c(
        "id_evento_atencao_saude",
        "faixa_etaria",
        "sexo"
      )
    )
  ) |> collapse::funique(cols = c("id_evento_atencao_saude", "mes"))

  data.table::setkey(base_det, id_evento_atencao_saude, mes)

  data.table::setkey(base_cons,  id_evento_atencao_saude, mes)

  base_amb <- data.table::merge.data.table(
    base_det,
    base_cons,
    all.x = TRUE,
    on = c("id_evento_atencao_saude", "mes")
  )

  # join por dicionário ----

  base_amb <- base_amb[
    cd_tabela_referencia != 0 &
      cd_tabela_referencia != 9 &
      cd_tabela_referencia != 98,
    !"cd_tabela_referencia"
  ][
    cd_procedimento %in% tabelas$cd_procedimento
  ] # filtrando tabelas e procedimentos sem informações

  base_amb <- data.table::merge.data.table(
    base_amb,
    tabelas,
    by = "cd_procedimento",
    all.x = TRUE
  )

  # lista de procedimentos disponíveis ----

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

  # estatísticas por estado ----

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
    ,
    furrr::future_map(
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

  # estatísticas por faixa etária ----

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
    furrr::future_map(
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

  # estatísticas por sexo ----

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
    furrr::future_map(
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

for (i in 11:15) {
  furrr::future_walk(
    i,
    ~ amb(urls, .x, tabelas)
  )

  print(i)

  gc()

}

# corrigindo exportações --------------------------------------------------

fix_db <- function(path, factors) {
  df <- readr::read_csv(path) |>
    dplyr::distinct(cd_procedimento, termo, categoria) |>
    data.table::as.data.table()

  df <- df[
    ,
    .SD[.(categoria = factors),
      on = "categoria"
    ],
    by = .(cd_procedimento, termo) # completando categorias faltantes
  ][
    ,
    furrr::future_map(
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
  uf = estados
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
