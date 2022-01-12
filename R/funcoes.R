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

# função de descompactação e leitura de arquivos do ftp

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

# função que cria dummies que indicam as tabelas base

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

# função que lê todos os .csv de uma pasta

load_data <- function(x) {
  fs::dir_ls(x, regexp = "*.csv") |>
    purrr::map(readr::read_delim,
      delim = ";"
    )
}

# not in

`%not_in%` <- Negate(`%in%`)
