# --------------- #
# --- FUNÇÕES --- #
# --------------- #

# função para seleção de bases por ano, estado, mês e período -------------

base <- function(ano, estado, mes, base, url_base, proc) {
  urls <- tibble::tibble(
    a = glue::glue("{url_base}{ano}/"),
    b = estado,
    c = glue::glue("_{ano}"),
  ) |>
    dplyr::group_by(a, b, c) |>
    tidyr::nest() |>
    dplyr::mutate(
      d = glue::glue("/{b}"),
      data = purrr::map(
        data,
        ~ tibble::tibble(
          e = mes,
          f = glue::glue("_{proc}_{base}.zip")
        )
      )
    ) |>
    tidyr::unnest(cols = c(data)) |>
    tidyr::unite("url",
      c(a, b, d, c, e, f),
      sep = ""
    ) |>
    purrr::flatten_chr()

  return(urls)
}

# função de descompactação, leitura e escrita na database (hosp) ----------

unpack_write_parquet <- function(url, cols) {
  temp <- tempfile(fileext = ".zip")

  download.file(
    url = url,
    destfile = temp,
    method = "auto",
    quiet = TRUE
  )

  df <- vroom::vroom(
    file = temp,
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

  df[, id_evento_atencao_saude := as.character(id_evento_atencao_saude)]

  if (stringr::str_detect(url, "DET")) {
    df[
      (ind_tabela_propria == 1 | cd_tabela_referencia %in% c("98", "90")),
      cd_procedimento := 0
    ][
      ,
      c("ano", "mes") := data.table::tstrsplit(get(cols[2]), "-", fixed = TRUE)
    ][
      ,
      cd_procedimento := data.table::fifelse(
        cd_procedimento == "0",
        "sem_info",
        as.character(as.numeric(cd_procedimento))
      )
    ]

    data.table::set(df, , c("ind_tabela_propria", "cd_tabela_referencia", cols[2]), NULL)
  }

  name <- stringr::str_extract(url, "(?<=/[A-Z]{2}/)(.*)(?=\\.zip$)")

  arrow::write_parquet(
    x = df,
    sink = glue::glue("data/parquet/{name}.parquet")
  )

  fs::file_delete(glue::glue("{temp}"))
}

# função de merge mês a mês -----------------------------------------------

merge_db <- function(path_1, path_2, termos) {
  db_1 <- arrow::read_parquet(path_1) |>
    data.table::as.data.table(key = "id_evento_atencao_saude")

  db_2 <- arrow::read_parquet(path_2) |>
    data.table::as.data.table(key = "id_evento_atencao_saude")

  termos <- data.table::as.data.table(termos, key = "cd_procedimento")

  db_3 <- data.table::merge.data.table(
    x = db_1,
    y = db_2,
    by = "id_evento_atencao_saude",
    all.x = TRUE
  )

  data.table::setkey(db_3, "cd_procedimento")

  db_3 <- data.table::merge.data.table(
    x = db_3,
    y = termos,
    by = "cd_procedimento",
    all.x = TRUE
  )

  db_3[
    ,
    termo := data.table::fifelse(
      cd_procedimento == "sem_info",
      "sem_info",
      termo
    )
  ]

  name <- stringr::str_extract(
    path_1,
    "(?<=/parquet/)(.*)(?=\\_DET.parquet$)"
  )

  db <- stringr::str_to_lower(
    stringr::str_extract(
      name,
      "([:alpha:]{3}|[:alpha:]{4})$"
    )
  )

  arrow::write_parquet(db_3, glue::glue("data/proc_{db}_db/{name}.parquet"))

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

export_parquet <- function(x, complete_vars, db_name, export_name, type, months) {
  db <- arrow::open_dataset(glue::glue("data/{db_name}"))

  group_by_var <- as.symbol(x)

  pbapply::pblapply(
    months,
    function(i) {
      df <- db |>
        dplyr::filter(mes == i) |>
        dplyr::group_by(cd_procedimento, termo, ano, mes, {{ group_by_var }}) |>
        dplyr::summarise(
          tot_qt = sum(qt_item_evento_informado, na.rm = TRUE),
          tot_vl = sum(vl_item_evento_informado, na.rm = TRUE)
        ) |>
        dplyr::ungroup() |>
        dplyr::mutate(mean_vl = tot_vl / tot_qt) |>
        dplyr::compute()

      proc_n_nulos <- df |>
        dplyr::group_by(cd_procedimento) |>
        dplyr::summarise(
          tot_qt = sum(tot_qt, na.rm = TRUE),
          tot_vl = sum(tot_vl, na.rm = TRUE),
          mean_vl = sum(mean_vl, na.rm = TRUE)
        ) |>
        dplyr::filter(tot_qt != 0 & tot_vl != 0 & mean_vl != 0) |>
        dplyr::select(cd_procedimento)

      df <- df |>
        dplyr::semi_join(proc_n_nulos, by = "cd_procedimento") |>
        dplyr::collect() |>
        data.table::as.data.table()

      if (x == "uf_prestador") {
        df <- df[
          ,
          .SD[
            .(uf_prestador = complete_vars),
            on = x
          ],
          by = .(cd_procedimento, termo, ano, mes) # completando UF's faltantes
        ][
          ,
          tipo := x
        ]
      } else if (x == "faixa_etaria") {
        df <- df[
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
          by = .(cd_procedimento, termo, ano, mes) # completando faixas etárias faltantes
        ][
          ,
          tipo := x
        ]
      } else {
        df <- df[
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
          by = .(cd_procedimento, termo, ano, mes) # completando sexos faltantes
        ][
          ,
          tipo := x
        ]
      }

      df <- df[
        ,
        furrr::future_map(
          .SD,
          collapse::replace_NA,
          value = 0
        )
      ]

      data.table::setnames(
        df,
        old = paste0(x),
        new = "categoria"
      )

      fs::dir_create("output/export")

      arrow::write_parquet(df, glue::glue("output/export/{export_name}_{i}.parquet"))
    }
  )

  gc()
}
