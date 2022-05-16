# ----------------- #
# --- FUNCTIONS --- #
# ----------------- #

# função de criação de urls do ftp ----------------------------------------

urls_ftp_ans <- function(year, uf, months, base, ftp_url, proc) {
  urls <- tibble::tibble(
    a = glue::glue("{ftp_url}{year}/"),
    b = uf,
    c = glue::glue("_{year}"),
  ) |>
    dplyr::group_by(a, b, c) |>
    tidyr::nest()

  if (base == "DET") {
    urls <- urls |>
      dplyr::mutate(
        d = glue::glue("/{b}"),
        data = purrr::map(
          data,
          ~ tibble::tibble(
            e = months,
            f = glue::glue("_{proc}_{base}.zip"),
            date = glue::glue("{months}-{year}")
          )
        )
      )
  } else {
    urls <- urls |>
      dplyr::mutate(
        d = glue::glue("/{b}"),
        data = purrr::map(
          data,
          ~ tibble::tibble(
            e = months,
            f = glue::glue("_{proc}_{base}.zip")
          )
        )
      )
  }

  urls <- urls |>
    tidyr::unnest(cols = c(data)) |>
    tidyr::unite("url",
      c(a, b, d, c, e, f),
      sep = ""
    )

  return(urls)
}

# função de descompactação, leitura e escrita em parquets -----------------

unpack_write <- function(url, date, cols) {
  tempdir <- fs::dir_create(glue::glue("{tempdir()}/downloads"))

  temp <- tempfile(fileext = ".zip", tmpdir = tempdir)

  download.file(
    url = url,
    destfile = temp,
    method = "auto",
    quiet = TRUE
  )

  zip::unzip(
    zipfile = temp,
    exdir = tempdir
  )

  fs::file_delete(temp)

  name <- stringr::str_extract(url, "(?<=/[A-Z]{2}/)(.*)(?=\\.zip$)")

  df <- vroom::vroom(
    file = glue::glue("{tempdir}/{name}.csv"),
    delim = ";",
    locale = locale(
      grouping_mark = ".",
      decimal_mark = ",",
      encoding = "UTF-8"
    ),
    col_select = stringr::str_to_upper(cols),
    skip_empty_rows = TRUE,
    progress = FALSE,
    show_col_types = FALSE
  ) |>
    janitor::clean_names() |>
    data.table::as.data.table()

  if (stringr::str_detect(url, "DET")) {
    m_y <- stringr::str_split(date, pattern = "-") |> purrr::flatten_chr()

    df <- df[
      ,
      ':='(
        mes = m_y[1],
        ano = m_y[2]
      )
    ][
      (ind_tabela_propria == 1 | cd_tabela_referencia %in% c("98", "90")),
      cd_procedimento := 0
    ][
      ,
      cd_procedimento := data.table::fifelse(
        cd_procedimento == "0",
        "sem_info",
        as.character(as.numeric(cd_procedimento))
      )
    ][
      ,
      !c("ind_tabela_propria", "cd_tabela_referencia")
    ]
  }

  if (stringr::str_detect(url, "CONS")) {
    df[
      ,
      faixa_etaria := tidyfast::dt_case_when(
        faixa_etaria == "<1" ~ "< 1",
        faixa_etaria == "80 ou mais" ~ "80 <",
        faixa_etaria == "Não identificado" ~ "N. I.",
        TRUE ~ faixa_etaria
      )
    ][
      ,
      sexo := tidyfast::dt_case_when(
        sexo %not_in% c("Masculino", "Feminino") ~ "N. I.",
        TRUE ~ sexo
      )
    ]
  }

  df[, id_evento_atencao_saude := as.character(id_evento_atencao_saude)]

  arrow::write_parquet(
    x = df,
    sink = glue::glue("data/parquet/{name}.parquet")
  )

  fs::file_delete(glue::glue("{tempdir}/{name}.csv"))
}

# função de merge mês a mês -----------------------------------------------

merge_parquets <- function(path_1, path_2) {

  db_1 <- arrow::open_dataset(path_1)

  db_2 <- arrow::open_dataset(path_2)

  db_3 <- db_1 |>
    dplyr::left_join(
      db_2,
      by = "id_evento_atencao_saude"
    ) |>
    dplyr::compute()

  name <- stringr::str_extract(
    path_1,
    "(?<=/parquet/)(.*)(?=\\_DET.parquet$)"
  )

  db_name <- stringr::str_to_lower(
    stringr::str_extract(
      name,
      "([:alpha:]{3}|[:alpha:]{4})$"
    )
  )

  arrow::write_parquet(
    db_3,
    glue::glue("data/{db_name}_db/{name}.parquet")
  )

  purrr::walk(
    list(db_1, db_2),
    ~ rm(.x)
  )

  gc()

  purrr::walk(
    list(path_1, path_2),
    fs::file_delete
  )
}

# função que cria dummies que indicam as tabelas base ---------------------

bind <- function(a, b, c, d) {
  df <- list(
    a = a |> dplyr::mutate(tabela = "19"),
    b = b |> dplyr::mutate(tabela = "20"),
    c = c |> dplyr::mutate(tabela = "22"),
    d = d |> dplyr::mutate(tabela = "63")
  ) |>
    dplyr::bind_rows()

  return(df)
}

# operador "not_in" -------------------------------------------------------

`%not_in%` <- Negate(`%in%`)

# função de tratamento da database do shinyapp ----------------------------

export_parquets <- function(x, db_name, export_name, months) {
  db <- arrow::open_dataset(glue::glue("data/{db_name}"))

  group_by_var <- as.symbol(x)

  pbapply::pblapply(
    months,
    function(i) {
      df <- db |>
        dplyr::filter(mes == i) |>
        dplyr::group_by(cd_procedimento, ano, mes, {{ group_by_var }}) |>
        dplyr::summarise(
          tot_qt = sum(qt_item_evento_informado, na.rm = TRUE),
          tot_vl = sum(vl_item_evento_informado, na.rm = TRUE)
        ) |>
        dplyr::ungroup() |>
        dplyr::mutate(
          mean_vl = tot_vl / tot_qt,
          tipo = x
        ) |>
        dplyr::collect() |>
        data.table::as.data.table()

      data.table::setnames(
        df,
        old = paste0(x),
        new = "categoria"
      )

      arrow::write_parquet(
        df,
        glue::glue("output/export/{export_name}_{i}.parquet")
      )
    }
  )

  gc()
}
