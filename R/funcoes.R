# função para seleção de bases por ano, estado, mês e período

base <- function(ano, estado, mes, base){
  
  x <- tibble::tibble(
    a = glue::glue("http://ftp.dadosabertos.ans.gov.br/FTP/PDA/TISS/HOSPITALAR/{ano}/"),
    b = estado,
    c = glue::glue("_{ano}"),
  ) |> # criando tibble base para operação
    dplyr::group_by(a, b, c) |>
    tidyr::nest() |> # criando subtibbles para cada região
    dplyr::mutate(
      d = glue::glue("/{b}"), # adicionando coluna auxiliar para união de url
      data = purrr::map(
        data,
        ~ tibble(
          e = mes, # criando coluna de meses em cada subtiblle
          f = glue::glue("_HOSP_{base}.zip")
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

unpack_read <- function(url, cols){
  
  temp <- tempfile()
  
  tempdir <- tempdir()
  
  temp <- curl::curl_download(url = url,
                              destfile = glue::glue("{temp}.zip"))
  
  temp <- unzip(zipfile = temp,
                exdir = tempdir)
  
  gc()
  
  x <- fread(input = temp,
             encoding = "UTF-8",
             select = cols,
             sep = ";",
             dec = ",")
  
  return(x)
}

# função que cria dummies indicando as tabelas base

bind <- function(a, b, c, d){
  
  a <- a |> 
    dplyr::mutate(tabela = "19")
  
  b <- b |> 
    dplyr::mutate(tabela = "20")
  
  c <- c |> 
    dplyr::mutate(tabela = "22")
  
  d <- d |> 
    dplyr::mutate(tabela = "63")
  
  x <- dplyr::bind_rows(a, b, c, d)
  
  return(x)
  
}

# função que lê todos os .csv de uma pasta

load_data <- function(x) {
  fs::dir_ls(x, regexp = "*.csv") |>
    purrr::map(readr::read_delim,
               delim = ";")
}