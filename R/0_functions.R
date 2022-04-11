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
          f = glue::glue("_{proc}_{base}.zip")
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

unpack_write_parquet <- function(url, cols) {
  temp <- tempfile(fileext = ".zip")

  tempdir <- tempdir()

  download.file(
    url = url,
    destfile = temp,
    method = "auto",
    quiet = TRUE
  )

  csv_file <- zip::unzip(
    zipfile = temp,
    exdir = tempdir
  )

  x <- data.table::fread(
    input = csv_file,
    encoding = "UTF-8",
    select = stringr::str_to_upper(cols),
    sep = ";",
    dec = ",",
    blank.lines.skip = TRUE,
    colClasses = list(double = stringr::str_to_upper(cols[1]))
  ) |>
    janitor::clean_names()

  x <- x[
    ,
    c("ano", "mes") := data.table::tstrsplit(ano_mes_evento, "-", fixed = TRUE)
  ][
    ,
    !"ano_mes_evento"
  ][
    ,
    id_evento_atencao_saude := as.character(id_evento_atencao_saude)
  ]

  # |>
  #   collapse::funique(cols = cols)

  if ("ind_tabela_propria" %in% names(x) == TRUE) x <- x[ind_tabela_propria != 1, !"ind_tabela_propria"]

  if ("cd_procedimento" %in% names(x) == TRUE) x[, cd_procedimento := as.character(as.double(cd_procedimento))]

  name <- stringr::str_extract(url, "(?<=/[A-Z]{2}/)(.*)(?=\\.zip$)") # match do nome entre a UF e o .zip no final da URL

  arrow::write_parquet(
    x = x,
    sink = glue::glue("data/parquet/{name}.parquet")
  )

  purrr::walk(
    list(temp, csv_file),
    ~ fs::file_delete(glue::glue("{.x}"))
  )
}

# função de merge mês a mês -----------------------------------------------

merge_db <- function(path_1, path_2, termos) {
  db_1 <- arrow::read_parquet(path_1) |>
    data.table::as.data.table(key = c("id_evento_atencao_saude", "mes", "ano"))

  db_2 <- arrow::read_parquet(path_2) |>
    data.table::as.data.table(key = c("id_evento_atencao_saude", "mes", "ano"))

  termos <- termos |>
    dplyr::mutate(cd_procedimento = as.character(as.numeric(cd_procedimento))) |>
    data.table::as.data.table(key = "cd_procedimento")

  db_3 <- data.table::merge.data.table(
    db_1, db_2,
    by = c("id_evento_atencao_saude", "mes", "ano"),
    all.x = TRUE
  )

  data.table::setkey(db_3, "cd_procedimento")

  db_3 <- data.table::merge.data.table(
    db_3, termos,
    by = "cd_procedimento",
    all.x = TRUE
  )

  # db_3 <- db_3[!is.na(termo)]

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
  x <- list(
    a = a |> dplyr::mutate(tabela = "19"),
    b = b |> dplyr::mutate(tabela = "20"),
    c = c |> dplyr::mutate(tabela = "22"),
    d = d |> dplyr::mutate(tabela = "63")
  ) |>
    dplyr::bind_rows()

  return(x)
}

# operador "not_in" -------------------------------------------------------

`%not_in%` <- Negate(`%in%`)

# função de tratamento da database do shinyapp ----------------------------

export_parquet <- function(x, complete_vars, db_name, export_name, type) {
  df <- arrow::open_dataset(glue::glue("data/{db_name}"))

  group_by_var <- as.symbol(x)

  mes <- c(paste0("0", 1:9), 10:12)

  pbapply::pblapply(
    mes,
    function(i) {
      y <- df |>
        dplyr::select(-tabela) |>
        dplyr::filter(mes == i) |>
        dplyr::group_by(cd_procedimento, termo, {{ group_by_var }}) |>
        dplyr::summarise(
          tot_qt = sum(qt_item_evento_informado, na.rm = TRUE),
          tot_vl = sum(vl_item_evento_informado, na.rm = TRUE)
        ) |>
        dplyr::ungroup() |>
        dplyr::mutate(mean_vl = tot_vl / tot_qt)

      proc_nulo <- y |>
        dplyr::group_by(cd_procedimento) |>
        dplyr::summarise(
          tot_qt = sum(tot_qt),
          tot_vl = sum(tot_vl),
          mean_vl = sum(mean_vl)
        ) |>
        dplyr::filter(tot_qt != 0 & tot_vl != 0 & mean_vl != 0) |>
        dplyr::select(cd_procedimento)

      z <- y |>
        dplyr::semi_join(proc_nulo, by = "cd_procedimento") |>
        dplyr::collect() |>
        data.table::as.data.table()

      if (x == "uf_prestador") {
        z <- z[
          ,
          .SD[
            .(uf_prestador = complete_vars),
            on = x
          ],
          by = .(cd_procedimento, termo) # completando UF's faltantes
        ]
      } else if (x == "faixa_etaria") {
        z <- z[
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
        z <- z[
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

      z <- z[
        ,
        furrr::future_map(
          .SD,
          collapse::replace_NA,
          value = 0
        )
      ][
        termo != "0"
      ][
        ,
        ":="(
          mes = as.integer(i),
          ano = as.integer(stringr::str_extract(export_name, "[0-9]{4}$")),
          tipo = type
        )
      ]

      data.table::setnames(
        z,
        old = paste0(x),
        new = "categoria"
      )

      arrow::write_parquet(z, glue::glue("output/{export_name}_{i}.parquet"))
    }
  )

  gc()
}
