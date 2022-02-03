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
          mes = as.numeric(mes),
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

# função de tratamento de dados mensal

tratamento_por_mes <- function(url_det, url_cons, cols, tabelas) {

  download_export <- function(i){
    url_det <- url_det |>
      dplyr::filter(mes == i)

    url_cons <- url_cons |>
      dplyr::filter(mes == i)

    temp1 <- tempfile()

    temp2 <- tempfile()

    tempdir <- tempdir()

    # download, descompactação e leitura ----

    purrr::walk2(
      .x = c(url_det$url, url_cons$url),
      .y = c(temp1, temp2),
      ~ download.file(
        url = .x,
        destfile = .y,
        method = "auto",
        quiet = TRUE
      )
    )

    csv_files <- purrr::map_chr(
      .x = c(temp1, temp2),
      ~ unzip(
        zipfile = .x,
        exdir = tempdir
      )
    )

    x <- purrr::map2(
      .x = csv_files,
      .y = cols,
      ~ data.table::fread(
        input = .x,
        encoding = "UTF-8",
        select = stringr::str_to_upper(.y),
        sep = ";",
        dec = ","
      ) |>
        janitor::clean_names()
    )

    x <- x |>
      purrr::map(
        ~ .x[
          ,
          collapse::na_omit(.x)
        ][
          ,
          mes := i
        ]
    )

    # joins ----

    purrr::walk(
      x,
      ~ data.table::setkey(.x, id_evento_atencao_saude, mes)
    )

    base_amb <- data.table::merge.data.table(
      x[[1]],
      x[[2]],
      all.x = TRUE,
      on = c("id_evento_atencao_saude", "mes")
    ) |> collapse::funique(c(
      "id_evento_atencao_saude",
      "cd_procedimento",
      "mes"))

    # join por dicionário

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
      termo := stringr::str_to_upper(termo)
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
      termo := stringr::str_to_upper(termo)
    ][
      ,
      purrr::map(
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
    ]    [
      ,
      termo := stringr::str_to_upper(termo)
    ][
      ,
      faixa_etaria := tidyfast::dt_case_when(
        faixa_etaria == "<1" ~ "< 1",
        faixa_etaria == "80 ou mais" ~ "80 <",
        TRUE ~ faixa_etaria
      )
    ][
      ,
      purrr::map(
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
      termo := stringr::str_to_upper(termo)
    ][
      ,
      purrr::map(
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

    purrr::walk(
      c(temp1, temp2, csv_files[1], csv_files[2]),
      ~ fs::file_delete(glue::glue("{.x}"))
    )

    gc()
  }

  pbapply::pblapply(
    X = 1:12,
    download_export,
    cl = parallel::detectCores()
  )

}

# operador "not_in"

`%not_in%` <- Negate(`%in%`)

# parâmetros --------------------------------------------------------------

# future::plan(multisession) # habilitando multithread

# memory.size(max = 10^12)

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

tabelas <- dplyr::tbl(con, "tabelas_tuss") |>
  dplyr::collect()

duckdb::dbDisconnect(con, shutdown = TRUE)

# exportando dados --------------------------------------------------------

purrr::walk(
  .x = 1:27,
  ~ tratamento_por_mes(
    url_det = urls[[1]][[.x]],
    url_cons = urls[[2]][[.x]],
    cols = list(
      det = c(
        "cd_procedimento",
        "id_evento_atencao_saude",
        "uf_prestador",
        "cd_tabela_referencia",
        "qt_item_evento_informado",
        "vl_item_evento_informado"
      ),
      cons = c(
        "id_evento_atencao_saude",
        "faixa_etaria",
        "sexo"
      )
    ),
    tabelas = tabelas
  )
)

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
    purrr::map(
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

