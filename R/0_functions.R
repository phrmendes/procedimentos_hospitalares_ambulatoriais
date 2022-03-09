# --------------- #
# --- FUNÇÕES --- #
# --------------- #

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
          f = glue::glue("_{proc}_{base}.zip"),
          mes = mes
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

# função de descompactação, leitura e escrita na database (hosp) ----------

unpack_write_parquet <- function(url, mes_url, cols, indexes) {
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
    ":="(
      mes = mes_url,
      id_evento_atencao_saude = as.character(id_evento_atencao_saude))
  ]

  if ("cd_procedimento" %in% names(x) == TRUE) {
    x <- collapse::funique(x, cols = c("id_evento_atencao_saude", "cd_procedimento", "mes"))

    x[
      ,
      ":="(
        qt_item_evento_informado = as.double(qt_item_evento_informado),
        vl_item_evento_informado = as.double(vl_item_evento_informado)
      )
    ]
  } else {
    x <- collapse::funique(x, cols = c("id_evento_atencao_saude", "mes"))
  }

  if ("cd_tabela_referencia" %in% names(x) == TRUE) x <- x[!(cd_tabela_referencia %in% c(0, 9, 98)), !"cd_tabela_referencia"]

  name <- stringr::str_extract(url, "(?<=/[A-Z]{2}/)(.*)(?=\\.zip$)") # match do nome entre a UF e o .zip no final da URL

  arrow::write_parquet(
    x = x,
    sink = glue::glue("data/parquet/{name}.parquet")
  )

  purrr::walk(
    list(temp, csv_file),
    ~ fs::file_delete(glue::glue("{.x}"))
  )

  gc()
}


# função para lidar com a base gigantesca de dados ambulatoriais ----------

parquet_amb <- function(path_1, path_2, termos) {
  db_1 <- arrow::read_parquet(path_1) |>
    data.table::as.data.table(key = c("id_evento_atencao_saude", "mes"))

  db_2 <- arrow::read_parquet(path_2) |>
    data.table::as.data.table(key = c("id_evento_atencao_saude", "mes"))

  termos <- termos |>
    data.table::as.data.table(key = "cd_procedimento")

  db_3 <- data.table::merge.data.table(
    db_1, db_2,
    by = c("id_evento_atencao_saude", "mes"),
    all.x = TRUE
  )

  data.table::setkey(db_3, "cd_procedimento")

  db_3 <- db_3[
    termos,
    on = "cd_procedimento"
  ]

  name <- stringr::str_extract(path_1, "(?<=/parquet/)(.*)(?=\\_AMB_DET.parquet$)")

  arrow::write_parquet(db_3, glue::glue("data/proc_amb_db/{name}.parquet"))

  gc()
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

export_parquet <- function(x, complete_vars, db_name, export_name) {
  df <- arrow::open_dataset(glue::glue("data/{db_name}"))

  group_by_var <- as.symbol(x)

  y <- df |>
    dplyr::group_by(cd_procedimento, termo, {{ group_by_var }}) |>
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

  termos_nulos <- y[
    ,
    lapply(.SD, sum),
    .SDcols = c("tot_qt", "tot_vl", "mean_vl"),
    by = .(termo)
  ][
    ,
    .(tot = sum(tot_qt, tot_vl, mean_vl)),
    by = .(termo)
  ][
    tot == 0,
    "termo"
  ] |>
    purrr::flatten_chr()

  y <- y[termo %not_in% termos_nulos]

  arrow::write_parquet(y, glue::glue("output/{export_name}.parquet"))

  gc()
}
