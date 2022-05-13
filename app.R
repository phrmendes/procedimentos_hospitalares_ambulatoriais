# ---------------------------------------------------------- #
# --- SHINY - PROCEDIMENTOS HOSPITALARES E AMBULATORIAIS --- #
# ---------------------------------------------------------- #

# bibliotecas e parâmetros ------------------------------------------------

library(tidyverse)
library(glue)
library(bs4Dash)
library(plotly)
library(MetBrewer)
library(shiny)
library(sf)
library(arrow)
library(duckdb)
library(tmaptools)
library(rlang)
library(shinycssloaders)
library(sysfonts)
library(writexl)

options(scipen = 999)
options(dplyr.summarise.inform = FALSE)
sysfonts::font_add_google("Open Sans")

# variáveis ---------------------------------------------------------------

geom_ufs <- readr::read_rds("output/geom_ufs.rds")

termos <- arrow::open_dataset("output/db_termos_shiny/")

db <- arrow::open_dataset("output/db_shiny/")

vars_shiny <- list(
  categoria = c("Faixa etária", "Sexo", "UF Prestador"),
  estatistica = c("Quantidade total", "Valor total", "Valor médio")
)

categorias <- fs::dir_ls("data/aux_files/", regexp = "*.csv") |>
  purrr::map(readr::read_csv, show_col_types = FALSE)

names(categorias) <- stringr::str_extract(names(categorias), "(?<=files/)(.*)(?=\\.csv)")

# adicionar tabela com os 10 procedimentos mais realizados no ano

# header ------------------------------------------------------------------

header <- bs4Dash::dashboardHeader(
  title = tags$a(
    tags$style(
      type = "text/css",
      ".shiny-output-error { visibility: hidden; }",
      ".shiny-output-error:before { visibility: hidden; }"
    )
  )
)

# sidebar -----------------------------------------------------------------

sidebar <- bs4Dash::dashboardSidebar(
  disable = TRUE,
  collapsed = TRUE,
  bs4Dash::sidebarMenu()
)

# body --------------------------------------------------------------------

body <- bs4Dash::dashboardBody(
  tags$head(
    tags$style(
      HTML(
        ".navbar { display: none; }
        .main-header { display: none; }"
      )
    )
  ),
  h2(
    "Procedimentos Médicos e Ambulatoriais",
    style = "font-family: 'Open Sans', sans-serif;"
  ),
  shiny::fluidRow(
    shiny::column(
      width = 4,
      bs4Dash::box(
        width = NULL,
        collapsible = FALSE,
        style = "font-size:14px; font-family: 'Open Sans', sans-serif;",
        shiny::selectInput(
          inputId = "base_procedimentos",
          label = "Base de procedimentos",
          selected = NULL,
          choices = c("Hospitalares", "Ambulatoriais")
        ),
        shiny::selectizeInput(
          inputId = "procedimento",
          label = "Procedimento",
          selected = NULL,
          choices = NULL
        )
      )
    ),
    shiny::column(
      width = 4,
      bs4Dash::box(
        width = NULL,
        collapsible = FALSE,
        style = "font-size:14px; font-family: 'Open Sans', sans-serif;",
        shiny::selectInput(
          inputId = "ano",
          label = "Ano",
          selected = NULL,
          choices = 2018:2020
        ),
        shiny::selectInput(
          inputId = "categoria",
          label = "Categoria",
          selected = NULL,
          choices = vars_shiny$categoria
        )
      )
    ),
    shiny::column(
      width = 4,
      bs4Dash::box(
        width = NULL,
        collapsible = FALSE,
        style = "font-size:14px; font-family: 'Open Sans', sans-serif;",
        shiny::selectInput(
          inputId = "estatistica",
          label = "Estatística",
          selected = NULL,
          choices = vars_shiny$estatistica
        ),
        shiny::actionButton(
          inputId = "busca",
          label = "Busca",
          icon = shiny::icon("search")
        )
      )
    )
  ),
  shiny::fluidRow(
    bs4Dash::infoBoxOutput(
      width = 4,
      "proc"
    ),
    bs4Dash::infoBoxOutput(
      width = 4,
      "qtd_tot"
    ),
    bs4Dash::infoBoxOutput(
      width = 4,
      "vl_tot"
    )
  ),
  shiny::fluidRow(
    bs4Dash::box(
      collapsible = FALSE,
      shinycssloaders::withSpinner(plotly::plotlyOutput("bar")),
      width = 6,
      align = "center"
    ),
    bs4Dash::box(
      collapsible = FALSE,
      shinycssloaders::withSpinner(plotly::plotlyOutput("ts")),
      width = 6,
      align = "center"
    )
  ),
  shiny::fluidRow(
    shiny::conditionalPanel(
      condition = "input.categoria == 'UF Prestador' && input.button != 0",
      bs4Dash::box(
        collapsible = FALSE,
        shinycssloaders::withSpinner(plotly::plotlyOutput("map")),
        width = 12,
        align = "center"
      )
    )
  ),
  shiny::fluidRow(
    shiny::column(
      width = 3,
      bs4Dash::box(
        width = NULL,
        collapsible = FALSE,
        align = "center",
        shiny::downloadButton(
          outputId = "download_anual",
          label = "Dados anuais",
          icon = shiny::icon("download")
        ),
        shiny::downloadButton(
          outputId = "download_mensal",
          label = "Dados mensais",
          icon = shiny::icon("download")
        )
      )
    )
  )
)

# ui ----------------------------------------------------------------------

ui <- bs4Dash::dashboardPage(
  title = "Abramge",
  header, sidebar, body
)

# server ------------------------------------------------------------------

server <- function(input, output, session) {

  # reactive variables ----------------------------------------------------

  option_base_procedimentos <- shiny::eventReactive(
    input$busca,
    {
      input$base_procedimentos
    }
  )

  option_procedimento <- shiny::eventReactive(
    input$busca,
    {
      input$procedimento
    }
  )

  option_ano <- shiny::eventReactive(
    input$busca,
    {
      as.integer(input$ano)
    }
  )

  option_estatistica <- shiny::eventReactive(
    input$busca,
    {
      input$estatistica
    }
  )

  option_categoria <- shiny::eventReactive(
    input$busca,
    {
      input$categoria
    }
  )

  option_db <- shiny::reactive({
    if (option_base_procedimentos() == "Hospitalares") {
      db <- "hosp"
    } else {
      db <- "amb"
    }

    return(db)
  })

  infoboxes_db <- shiny::reactive({
    if (input$base_procedimentos == "Hospitalares") {
      db <- "hosp"
    } else {
      db <- "amb"
    }

    return(db)
  })

  # gc --------------------------------------------------------------------

  shiny::eventReactive(
    input$busca,
    {
      Sys.sleep(4)

      gc()
    }
  )

  # dados anuais ----------------------------------------------------------

  dados_anuais <- shiny::reactive({
    estatistica <- as.symbol(option_estatistica())

    cd_procedimento_termos <- termos |>
      dplyr::filter(
        db == option_db(),
        termo == option_procedimento(),
        ano == option_ano()
      ) |>
      dplyr::pull(cd_procedimento)

    dados_anuais <- db |>
      dplyr::filter(
        db == option_db(),
        tipo == janitor::make_clean_names(option_categoria()),
        cd_procedimento == cd_procedimento_termos,
        ano == option_ano()
      ) |>
      dplyr::rename(
        `Quantidade total` = tot_qt,
        `Valor total` = tot_vl,
        `Valor médio` = mean_vl
      ) |>
      dplyr::group_by(categoria)

    if (option_estatistica() %in% c(vars_shiny$estatistica[1:2])) {
      dados_anuais <- dados_anuais |>
        dplyr::summarise({{ estatistica }} := sum({{ estatistica }}))
    } else {
      dados_anuais <- dados_anuais |>
        dplyr::summarise({{ estatistica }} := mean({{ estatistica }}))
    }

    dados_anuais <- dados_anuais |>
      dplyr::collect()

    if (option_categoria() == "UF Prestador") {
      dados_anuais <- dados_anuais |>
        dplyr::mutate(categoria = factor(
          categoria,
          levels = purrr::flatten_chr(categorias$estados[1])
        ))
    } else if (option_categoria() == "Faixa etária") {
      dados_anuais <- dados_anuais |>
        dplyr::mutate(categoria = factor(
          categoria,
          levels = purrr::flatten_chr(categorias$faixas)
        ))
    } else {
      dados_anuais <- dados_anuais |>
        dplyr::mutate(categoria = factor(
          categoria,
          levels = purrr::flatten_chr(categorias$sexos)
        ))
    }

    dados_anuais <- dados_anuais |>
      tidyr::complete(categoria) |>
      dplyr::mutate({{ estatistica }} := tidyr::replace_na({{ estatistica }}, 0))

    if (option_categoria() == "Faixa etária") {
      dados_anuais <- dados_anuais |>
        dplyr::arrange(categoria)
    } else {
      dados_anuais <- dados_anuais |>
        dplyr::mutate(categoria := forcats::fct_reorder(categoria, {{ estatistica }})) |>
        dplyr::arrange(categoria)
    }

    return(dados_anuais)
  })

  # dados mensais ---------------------------------------------------------

  dados_mensais <- shiny::reactive({
    estatistica <- as.symbol(option_estatistica())

    cd_procedimento_termos <- termos |>
      dplyr::filter(
        db == option_db(),
        termo == option_procedimento(),
        ano == option_ano()
      ) |>
      dplyr::pull(cd_procedimento)

    dados_mensais <- db |>
      dplyr::filter(
        db == option_db(),
        tipo == janitor::make_clean_names(option_categoria()),
        cd_procedimento == cd_procedimento_termos,
        ano == option_ano()
      ) |>
      dplyr::rename(
        `Quantidade total` = tot_qt,
        `Valor total` = tot_vl,
        `Valor médio` = mean_vl
      ) |>
      dplyr::group_by(categoria, ano, mes)

    if (option_estatistica() %in% c(vars_shiny$estatistica[1:2])) {
      dados_mensais <- dados_mensais |>
        dplyr::summarise({{ estatistica }} := sum({{ estatistica }}))
    } else {
      dados_mensais <- dados_mensais |>
        dplyr::summarise({{ estatistica }} := mean({{ estatistica }}))
    }

    if (option_categoria() == "UF Prestador") {
      dados_mensais <- dados_mensais |>
        dplyr::left_join(
          categorias$estados,
          by = c("categoria" = "estados")
        ) |>
        dplyr::select(-categoria) |>
        dplyr::rename(categoria = regiao) |>
        dplyr::group_by(categoria, ano, mes)

      if (option_estatistica() %in% c(vars_shiny$estatistica[1:2])) {
        dados_mensais <- dados_mensais |>
          dplyr::summarise({{ estatistica }} := sum({{ estatistica }}))
      } else {
        dados_mensais <- dados_mensais |>
          dplyr::summarise({{ estatistica }} := mean({{ estatistica }}))
      }

      dados_mensais <- dados_mensais |>
        dplyr::collect() |>
        dplyr::mutate(categoria = factor(
          categoria,
          levels = unique(purrr::flatten_chr(categorias$estados[2]))
        ))
    } else if (option_categoria() == "Faixa etária") {
      dados_mensais <- dados_mensais |>
        dplyr::collect() |>
        dplyr::mutate(categoria = factor(
          categoria,
          levels = purrr::flatten_chr(categorias$faixas)
        ))
    } else {
      dados_mensais <- dados_mensais |>
        dplyr::collect() |>
        dplyr::mutate(categoria = factor(
          categoria,
          levels = purrr::flatten_chr(categorias$sexos)
        ))
    }

    dados_mensais <- dados_mensais |>
      dplyr::ungroup() |>
      dplyr::mutate(mes = factor(mes, levels = 1:12)) |>
      dplyr::group_by(ano) |>
      tidyr::complete(categoria, mes) |>
      dplyr::mutate({{ estatistica }} := tidyr::replace_na({{ estatistica }}, 0))

    return(dados_mensais)
  })

  # info boxes ------------------------------------------------------------

  db_name <- shiny::reactive({
    if (infoboxes_db() == "hosp") {
      db_name <- "hospitalares"
    } else {
      db_name <- "ambulatoriais"
    }

    return(db_name)
  })

  output$proc <- bs4Dash::renderInfoBox({
    input_ano <- as.integer(input$ano)

    n <- termos |>
      dplyr::filter(ano == input_ano & db == infoboxes_db()) |>
      dplyr::tally() |>
      dplyr::collect() |>
      dplyr::pull()

    bs4Dash::infoBox(
      title = shiny::HTML(glue::glue("Nº de procedimentos {db_name()} disponíveis para consulta:")),
      value = prettyNum(n, big.mark = "\\."),
      icon = shiny::icon("notes-medical", lib = "font-awesome"),
      color = "primary"
    )
  })

  output$qtd_tot <- bs4Dash::renderInfoBox({
    input_ano <- as.integer(input$ano)

    qtd_tot <- db |>
      dplyr::filter(
        ano == input_ano,
        db == infoboxes_db(),
        tipo == "sexo"
      ) |>
      dplyr::summarise(tot_qt = sum(tot_qt)) |>
      dplyr::pull()

    qtd_tot_pretty <- prettyNum(
      qtd_tot,
      big.mark = "\\.",
      decimal.mark = ","
    )

    n_dots <- stringr::str_count(qtd_tot_pretty, "\\.")

    if (n_dots >= 3) {
      qtd_tot <- prettyNum(
        round(qtd_tot / 10^9, 2),
        big.mark = "\\.",
        decimal.mark = ","
      )

      qtd_tot <- glue::glue("{qtd_tot} bilhões")
    } else if (n_dots < 3 & n_dots >= 2) {
      qtd_tot <- prettyNum(
        round(qtd_tot / 10^6, 2),
        big.mark = "\\.",
        decimal.mark = ","
      )

      qtd_tot <- glue::glue("{qtd_tot} milhões")
    } else if (n_dots < 2 & n_dots >= 1) {
      qtd_tot <- prettyNum(
        round(qtd_tot / 10^3, 2),
        big.mark = "\\.",
        decimal.mark = ","
      )

      qtd_tot <- glue::glue("{qtd_tot} mil")
    } else {
      qtd_tot <- glue::glue("{qtd_tot}")
    }

    bs4Dash::infoBox(
      title = shiny::HTML(glue::glue("Procedimentos {db_name()} realizados durante o ano:")),
      value = qtd_tot,
      icon = shiny::icon("calendar", lib = "font-awesome"),
      color = "orange"
    )
  })

  output$vl_tot <- bs4Dash::renderInfoBox({
    input_ano <- as.integer(input$ano)

    vl_tot <- db |>
      dplyr::filter(
        ano == input_ano,
        db == infoboxes_db(),
        tipo == "sexo"
      ) |>
      dplyr::summarise(vl_tot = sum(tot_vl)) |>
      dplyr::pull()

    vl_tot_pretty <- prettyNum(
      vl_tot,
      big.mark = "\\.",
      decimal.mark = ","
    )

    n_dots <- stringr::str_count(vl_tot_pretty, "\\.")

    if (n_dots >= 3) {
      vl_tot <- prettyNum(
        round(vl_tot / 10^9, 2),
        big.mark = "\\.",
        decimal.mark = ","
      )

      vl_tot <- glue::glue("R$ {vl_tot} bilhões")
    } else if (n_dots < 3 & n_dots >= 2) {
      vl_tot <- prettyNum(
        round(vl_tot / 10^6, 2),
        big.mark = "\\.",
        decimal.mark = ","
      )

      vl_tot <- glue::glue("R$ {vl_tot} milhões")
    } else if (n_dots < 2 & n_dots >= 1) {
      vl_tot <- prettyNum(
        round(vl_tot / 10^3, 2),
        big.mark = "\\.",
        decimal.mark = ","
      )

      vl_tot <- glue::glue("R$ {vl_tot} mil")
    } else {
      vl_tot <- glue::glue("R$ {vl_tot}")
    }

    bs4Dash::infoBox(
      title = shiny::HTML(glue::glue("Valor total gasto em procedimentos {db_name()} no ano:")),
      value = vl_tot,
      icon = shiny::icon("coins", lib = "font-awesome"),
      color = "lightblue"
    )
  })

  # bar plot --------------------------------------------------------------

  output$bar <- plotly::renderPlotly({
    plot_bar <- dados_anuais() |>
      ggplot() +
      geom_col(
        aes_string(
          x = names(dados_anuais())[1],
          y = glue::glue("`{names(dados_anuais())[2]}`"),
          fill = names(dados_anuais())[1]
        )
      ) +
      ggtitle(
        stringr::str_to_upper(
          glue::glue("{option_estatistica()} do procedimento por {option_categoria()} ({option_ano()})")
        )
      ) +
      theme_minimal() +
      xlab(glue::glue("{option_categoria()}")) +
      theme(
        legend.position = "none",
        text = element_text(
          size = 10,
          family = "Open Sans"
        ),
        plot.title = element_text(
          face = "bold",
          hjust = 0.5
        )
      ) +
      scale_fill_manual(
        values = MetBrewer::met.brewer(
          name = "Hokusai2",
          n = 27,
          type = "continuous"
        )
      )

    plotly::ggplotly(plot_bar)
  })

  # map plot --------------------------------------------------------------

  output$map <- plotly::renderPlotly({
    plot_map <- geom_ufs |>
      dplyr::left_join(
        dados_anuais(),
        by = "categoria"
      ) |>
      dplyr::select(-code_state)

    fill_var <- as.symbol(names(plot_map)[2])

    plot_map <- plot_map |>
      ggplot() +
      geom_sf(
        aes(
          geometry = geom,
          fill = {{ fill_var }}
        ),
        color = "black",
        size = 0.1
      ) +
      theme_minimal() +
      scale_fill_gradientn(
        colors = tmaptools::get_brewer_pal(
          "Blues",
          n = 28,
          plot = FALSE,
          contrast = c(.2, 1)
        )
      ) +
      theme(
        axis.title.x = element_blank(),
        axis.text.x = element_blank(),
        axis.ticks.x = element_blank(),
        axis.title.y = element_blank(),
        axis.text.y = element_blank(),
        axis.ticks.y = element_blank(),
        legend.position = "right",
        text = element_text(
          size = 10,
          family = "Open Sans"
        ),
        plot.title = element_text(
          face = "bold",
          hjust = 0.5
        )
      ) +
      ggtitle(
        stringr::str_to_upper(
          glue::glue(
            "{option_estatistica()} do procedimento por {option_categoria()} ({option_ano()})"
          )
        )
      )

    plotly::ggplotly(plot_map)
  })

  # time series plot -------------------------------------------

  output$ts <- plotly::renderPlotly({
    plot_line <- dados_mensais() |>
      dplyr::ungroup() |>
      dplyr::mutate(
        data = glue::glue("{mes}-{ano}") |>
          as.character() |>
          lubridate::my()
      ) |>
      dplyr::select(-c(ano, mes))

    plot_line <- plot_line |>
      ggplot() +
      geom_line(
        aes_string(
          x = names(plot_line)[3],
          y = glue::glue("`{names(plot_line)[2]}`"),
          group = names(plot_line)[1],
          color = names(plot_line)[1]
        ),
        size = 0.5
      ) +
      labs(
        x = "",
        y = option_estatistica(),
        color = ifelse(
          option_categoria() == "UF Prestador",
          "Região",
          option_categoria()
        )
      ) +
      theme_minimal() +
      theme(
        legend.position = "right",
        text = element_text(
          size = 10,
          family = "Open Sans"
        ),
        plot.title = element_text(
          face = "bold",
          hjust = 0.5
        )
      ) +
      scale_color_manual(values = tmaptools::get_brewer_pal("Paired", n = 13)) +
      ggtitle(
        stringr::str_to_upper(
          glue::glue(
            "{option_estatistica()} do procedimento por {option_categoria()} (jan - dez/{option_ano()})"
          )
        )
      )

    plotly::ggplotly(plot_line)
  })

  # downloads -------------------------------------------------------------

  output$download_anual <- shiny::downloadHandler(
    filename = function() {
      glue::glue("dados_{option_db()}_{janitor::make_clean_names(option_categoria())}_{janitor::make_clean_names(option_estatistica())}_{option_ano()}.xlsx")
    },
    content = function(file) {
      writexl::write_xlsx(dados_anuais(), file)
    }
  )

  output$download_mensal <- shiny::downloadHandler(
    filename = function() {
      glue::glue("dados_mensais_{option_db()}_{janitor::make_clean_names(option_categoria())}_{janitor::make_clean_names(option_estatistica())}_{option_ano()}.xlsx")
    },
    content = function(file) {
      writexl::write_xlsx(dados_mensais(), file)
    }
  )

  # server-side options ---------------------------------------------------

  shiny::observe({
    if (input$base_procedimentos == "Hospitalares") {
      options <- termos |>
        dplyr::filter(db == "hosp")
    } else {
      options <- termos |>
        dplyr::filter(db == "amb")
    }

    input_ano <- as.integer(input$ano)

    options <- options |>
      dplyr::filter(ano == input_ano) |>
      dplyr::select(termo) |>
      dplyr::collect() |>
      purrr::flatten_chr()

    shiny::updateSelectizeInput(
      session,
      inputId = "procedimento",
      choices = options,
      selected = NULL,
      server = TRUE
    )
  })
}

shiny::shinyApp(ui, server)
