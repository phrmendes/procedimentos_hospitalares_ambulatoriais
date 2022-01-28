# função para seleção de bases por ano, estado, mês e período -------------

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
          f = glue::glue("_{proc}_{base}.zip")
        )
      )
    ) |>
    tidyr::unnest(cols = c(data)) |>
    tidyr::unite("url",
      c(a, b, d, c, e, f),
      sep = ""
    ) |> # juntando tudo em uma url só
    purrr::flatten_chr() # criando um vetor de urls

  return(x)
}

# função de descompactação, leitura e escrita na database (hosp) ----------

unpack_write_db <- function(url, cols, name, indexes) {
  temp <- tempfile()

  tempdir <- tempdir()

  download.file(
    url = url,
    destfile = temp,
    method = "auto",
    quiet = TRUE
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
  ) |>
    janitor::clean_names()

  x <- x[
    ,
    collapse::na_omit(x)
  ][
    ,
    id_evento_atencao_saude := as.character(id_evento_atencao_saude)
  ]

  if (c("cd_tabela_referencia") %in% names(x) == TRUE) {
    x <- x[
      !(cd_tabela_referencia %in% c(0, 9, 98))
    ][
      cd_procedimento != "TABELA PRÓPRIA"
    ]
  }

  con <- duckdb::dbConnect(
    duckdb::duckdb(),
    dbdir = "input/proc.duckdb"
  )

  duckdb::dbWriteTable(
    conn = con,
    value = x,
    name = glue::glue("{name}"),
    overwrite = FALSE,
    temporary = FALSE,
    append = TRUE,
    indexes = indexes
  )

  purrr::walk(
    c(temp, csv_file),
    ~ fs::file_delete(glue::glue("{.x}"))
  )

  duckdb::dbDisconnect(con, shutdown = TRUE)
}

# função de descompactação e leitura (amb) --------------------------------

unpack_read <- function(url, cols) {
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
    select = cols,
    sep = ";",
    dec = ","
  )

  purrr::walk(
    c(temp, csv_file),
    ~ fs::file_delete(glue::glue("{.x}"))
  )

  gc()

  return(x)
}

# função que cria dummies que indicam as tabelas base ---------------------

bind <- function(a, b, c, d) {
  x <- list(
    a = a |> dplyr::mutate(tabela = "19"),
    b = b |> dplyr::mutate(tabela = "20"),
    c = c |> dplyr::mutate(tabela = "22"),
    d = d |> dplyr::mutate(tabela = "63")
  ) |>
    dplyr::bind_rows()

  return(x)
}

# função que lê todos os .csv de uma pasta --------------------------------

load_data <- function(x) {
  fs::dir_ls(x, regexp = "*.csv") |>
    purrr::map(
      readr::read_delim,
      delim = ";"
    )
}

# operador "not_in" -------------------------------------------------------

`%not_in%` <- Negate(`%in%`)

# função de tratamento da database do shinyapp ----------------------------

import_shinydb <- function(x, complete_vars, db_name, table) {
  con <- duckdb::dbConnect(
    duckdb::duckdb(),
    dbdir = "input/proc_hosp.duckdb"
  )

  shinydb <- duckdb::dbConnect(
    duckdb::duckdb(),
    dbdir = "output/shinydb.duckdb"
  )

  df <- dplyr::tbl(con, paste0(table))

  group_by_var <- as.symbol(glue::glue("{x}"))

  y <- df |>
    dplyr::group_by(cd_procedimento, termo, group_by_var) |>
    dplyr::summarise(
      tot_qt = sum(qt_item_evento_informado, na.rm = TRUE),
      tot_vl = sum(vl_item_evento_informado, na.rm = TRUE)
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      mean_vl = round(tot_vl / tot_qt, 2)
    ) |>
    dplyr::collect() |>
    data.table::as.data.table()

  y <- y[
    termo != "SEM INFORMAÇÕES",
    termo := furrr::future_map_chr(
      termo,
      stringr::str_to_upper
    )
  ]

  if (x == "uf_prestador") {
    y <- y[
      ,
      .SD[
        .(uf_prestador = complete_vars),
        on = x
      ],
      by = .(cd_procedimento, termo) # completando UF's faltantes
    ]
  } else if (x == "faixa_etaria") {
    y <- y[
      ,
      faixa_etaria := tidyfast::dt_case_when(
        faixa_etaria == "<1" ~ "< 1",
        faixa_etaria == "80 ou mais" ~ "80 <",
        TRUE ~ faixa_etaria
      )
    ][
      ,
      .SD[
        .(faixa_etaria = complete_vars),
        on = x
      ],
      by = .(cd_procedimento, termo) # completando faixas etárias faltantes
    ]
  } else {
    y <- y[
      ,
      sexo := tidyfast::dt_case_when(
        sexo %not_in% c("Masculino", "Feminino") ~ "N. I.",
        TRUE ~ sexo
      )
    ][
      ,
      .SD[
        .(sexo = complete_vars),
        on = x
      ],
      by = .(cd_procedimento, termo) # completando sexos faltantes
    ]
  }

  y <- y[
    ,
    furrr::future_map(
      .SD,
      collapse::replace_NA,
      value = 0
    )
  ][
    termo != "0"
  ]

  data.table::setnames(
    y,
    old = paste0(x),
    new = "categoria"
  )

  duckdb::dbWriteTable(
    conn = shinydb,
    value = y,
    name = db_name,
    overwrite = TRUE,
    temporary = FALSE
  )

  purrr::walk(
    c(con, shinydb),
    ~ duckdb::dbDisconnect(.x, shutdown = TRUE)
  )

  return("Importado!")
}
