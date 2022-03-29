# ---------------------------------------------------------- #
# --- SHINY - PROCEDIMENTOS HOSPITALARES E AMBULATORIAIS --- #
# ---------------------------------------------------------- #

# bibliotecas, funções e opções -------------------------------------------

# PASSAR QUERY PARQUET PARA QUERY DUCKDB

library(tidyverse)
library(glue)
library(shinydashboard)
library(plotly)
library(MetBrewer)
library(shiny)
library(sf)
library(arrow)
library(tmaptools)
library(rlang)
library(shinycssloaders)
library(ggthemes)
library(duckdb)

options(scipen = 999)
options(dplyr.summarise.inform = FALSE)

# variáveis ---------------------------------------------------------------

# geobr::read_state(
#   code_state = "all",
#   showProgress = FALSE
# ) |>
#   dplyr::select(code_state, abbrev_state, geom) |>
#   dplyr::rename(categoria = abbrev_state) |>
#   readr::write_rds("output/geom_ufs.rds")

geom_ufs <- readr::read_rds("output/geom_ufs.rds")

termos <- fs::dir_ls("output/", regexp = "termos")

vars_shiny <- purrr::map(
  termos,
  arrow::read_parquet
) |>
  `names<-`(stringr::str_extract(termos, "(?<=output/)(.*)(?=.parquet)"))

vars_shiny$categoria <- c("Faixa etária", "Sexo", "UF")

vars_shiny$estatistica <- c("Quantidade total", "Valor total", "Valor médio")

# header ------------------------------------------------------------------

reactlog::reactlog_enable()

header <- shinydashboard::dashboardHeader(
  title = tags$a(
    title = "Abramge",
    href = "https://www.abramge.com.br/",
    tags$img(
      src = "https://abramge.com.br/portal/templates/abramge/images/logo-abramge-55-anos.png",
      height = 45
    ), # nome e logo da Abramge
    tags$style(
      type = "text/css",
      ".shiny-output-error { visibility: hidden; }",
      ".shiny-output-error:before { visibility: hidden; }"
    ) # desativando mensagens de erro
  )
)

# sidebar -----------------------------------------------------------------

sidebar <- shinydashboard::dashboardSidebar(
  shinydashboard::sidebarMenu(
    shinydashboard::menuItem(
      "Dados de procedimentos",
      tabName = "proc",
      icon = icon("chart-bar")
    ),
    shinydashboard::menuItem(
      "Séries históricas",
      tabName = "ts",
      icon = icon("chart-line")
    )
  )
)

# body --------------------------------------------------------------------

body <- shinydashboard::dashboardBody(
  shinydashboard::tabItems(
    shinydashboard::tabItem(
      tabName = "ts",
      h2("Séries históricas"),
      shiny::fluidRow(
        shinydashboard::box(
          title = "Parâmetros",
          width = 6,
          solidHeader = TRUE,
          shiny::selectInput(
            inputId = "base_procedimentos_ts",
            label = "Selecione uma base de procedimentos",
            selected = NULL,
            choices = c("Hospitalares", "Ambulatoriais")
          ),
          shiny::selectizeInput(
            inputId = "ano_ts",
            label = "Selecione um ano",
            selected = NULL,
            choices = 2018:2020
          ),
          shiny::selectizeInput(
            inputId = "procedimento_ts",
            label = "Selecione um procedimento",
            selected = NULL,
            choices = NULL
          ),
          shiny::selectInput(
            inputId = "categoria_ts",
            label = "Selecione uma categoria",
            selected = NULL,
            choices = vars_shiny$categoria
          ),
          shiny::selectInput(
            inputId = "estatistica_ts",
            label = "Selecione uma estatística",
            selected = NULL,
            choices = vars_shiny$estatistica
          )
        )
      ),
      shiny::fluidRow(
        shinydashboard::box(
          shinycssloaders::withSpinner(plotly::plotlyOutput("ts")),
          width = 12,
          align = "center"
        )
      )
    ),
    shinydashboard::tabItem(
      tabName = "proc",
      h2("Estatísticas anuais"),
      shiny::fluidRow(
        shinydashboard::box(
          title = "Parâmetros",
          width = 6,
          solidHeader = TRUE,
          shiny::selectInput(
            inputId = "base_procedimentos",
            label = "Selecione uma base de procedimentos",
            selected = NULL,
            choices = c("Hospitalares", "Ambulatoriais")
          ),
          shiny::selectizeInput(
            inputId = "ano",
            label = "Selecione um ano",
            selected = NULL,
            choices = 2018:2020
          ),
          shiny::selectizeInput(
            inputId = "procedimento",
            label = "Selecione um procedimento",
            selected = NULL,
            choices = NULL
          ),
          shiny::selectInput(
            inputId = "categoria",
            label = "Selecione uma categoria",
            selected = NULL,
            choices = vars_shiny$categoria
          ),
          shiny::selectInput(
            inputId = "estatistica",
            label = "Selecione uma estatística",
            selected = NULL,
            choices = vars_shiny$estatistica
          )
        ),
        shiny::conditionalPanel(
          condition = "input.estatistica == 'Quantidade total' & input.categoria == 'UF'",
          shinydashboard::box(
            shinycssloaders::withSpinner(plotly::plotlyOutput("map")),
            width = 6,
            align = "center"
          )
        )
      ),
      shiny::fluidRow(
        shinydashboard::box(
          shinycssloaders::withSpinner(plotly::plotlyOutput("bar")),
          width = 12,
          align = "center"
        )
      )
    )
  )
)

# ui ----------------------------------------------------------------------

ui <- shinydashboard::dashboardPage(
  title = "Abramge", # título da aba do dashboard
  header, sidebar, body
)

# server ------------------------------------------------------------------

# input <- list(
#   base_procedimentos = "Hospitalares",
#   categoria = "UF",
#   estatistica = "Quantidade total",
#   procedimento = "CONSULTA EM CONSULTÓRIO (NO HORÁRIO NORMAL OU PREESTABELECIDO)",
#   ano = 2018
# )

server <- function(input, output, session) {
  dados_anuais  <- shiny::reactive({
    if (input$base_procedimentos == "Hospitalares") {
      dados_anuais <- fs::dir_ls("output/", regexp = "base_hosp(.*)\\.parquet") |>
        arrow::open_dataset()
    } else {
      dados_anuais <- fs::dir_ls("output/", regexp = "base_amb(.*)\\.parquet") |>
        arrow::open_dataset()
    }

    estatistica <- as.symbol(input$estatistica)

    dados_anuais <- dados_anuais |>
      dplyr::rename(
        `Quantidade total` = tot_qt,
        `Valor total` = tot_vl,
        `Valor médio` = mean_vl
      ) |>
      dplyr::filter(
        tipo == stringr::str_to_lower(input$categoria),
        ano == input$ano,
        termo == input$procedimento
      ) |>
      dplyr::group_by(cd_procedimento, termo, categoria) |>
      dplyr::summarise({{ estatistica }} := sum({{ estatistica }})) |>
      dplyr::ungroup() |>
      dplyr::select(categoria, {{ estatistica }}) |>
      dplyr::collect() |>
      tibble::as_tibble()

    return(dados_anuais)
  })

  dados_mensais <- shiny::reactive({
    if (input$base_procedimentos_ts == "Hospitalares") {
      dados_mensais <- fs::dir_ls("output/", regexp = "base_hosp(.*)\\.parquet") |>
        arrow::open_dataset()
    } else {
      dados_mensais <- fs::dir_ls("output/", regexp = "base_amb(.*)\\.parquet") |>
        arrow::open_dataset()
    }

    estatistica_ts <- as.symbol(input$estatistica_ts)

    dados_mensais <- dados_mensais |>
      dplyr::rename(
        `Quantidade total` = tot_qt,
        `Valor total` = tot_vl,
        `Valor médio` = mean_vl
      ) |>
      dplyr::filter(
        tipo == stringr::str_to_lower(input$categoria_ts),
        ano == input$ano_ts,
        termo == input$procedimento_ts
      ) |>
      dplyr::group_by(cd_procedimento, termo, categoria, mes) |>
      dplyr::summarise({{ estatistica_ts }} := sum({{ estatistica_ts }})) |>
      dplyr::ungroup() |>
      dplyr::select(categoria, mes, {{ estatistica_ts }}) |>
      dplyr::collect() |>
      tibble::as_tibble()

    return(dados_mensais)
  })

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
          glue::glue("{input$estatistica} de procedimentos por {input$categoria}")
        )
      ) +
      theme_minimal() +
      xlab(glue::glue("{input$categoria}")) +
      theme(legend.position = "none") +
      scale_fill_manual(values = MetBrewer::met.brewer(
        name = "Ingres",
        n = 27,
        type = "continuous"
      ))

    plotly::ggplotly(plot_bar)
  })

  output$map <- plotly::renderPlotly({
    plot_map <- geom_ufs |>
      dplyr::left_join(
        dados_anuais(),
        by = "categoria"
      ) |>
      ggplot() +
      geom_sf(
        aes(
          geometry = geom,
          fill = `Quantidade total`
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
      ) +
      ggtitle(
        stringr::str_to_upper(
          glue::glue(
            "{input$estatistica} de procedimentos por {input$categoria}"
          )
        )
      )

    plotly::ggplotly(plot_map)
  })

output$ts <- plotly::renderPlotly({
  plot_line <- dados_mensais() |>
    dplyr::mutate(
      mes = lubridate::my(paste0(mes, "-", input$ano_ts)),
      mes = lubridate::month(mes, label = TRUE),
      categoria = forcats::as_factor(categoria)
    ) |>
    ggplot() +
    geom_line(
      aes_string(
        x = names(dados_mensais())[2],
        y = glue::glue("`{names(dados_mensais())[3]}`"),
        group = names(dados_mensais())[1],
        color = names(dados_mensais())[1]
      ),
      size = 1
    ) +
    labs(y = "Mês", color = input$categoria) +
    ggthemes::theme_excel_new() +
    scale_fill_manual(values = MetBrewer::met.brewer(
      name = "Ingres",
      n = 27,
      type = "continuous"
    )) +
    ggtitle(
      stringr::str_to_upper(
        glue::glue(
          "{input$estatistica_ts} de procedimentos por {input$categoria_ts}"
        )
      )
    )

  plotly::ggplotly(plot_line)
})

  # opções server-side

  shiny::observe({
    if (input$base_procedimentos == "Hospitalares") {
      options <- vars_shiny[names(vars_shiny) == paste0("termos_hosp_", input$ano)] |>
        purrr::flatten() |>
        purrr::flatten_chr()
    } else {
      options <- vars_shiny[names(vars_shiny) == paste0("termos_amb_", input$ano)] |>
        purrr::flatten() |>
        purrr::flatten_chr()
    }

    shiny::updateSelectizeInput(
      session,
      inputId = "procedimento",
      choices = options,
      selected = NULL,
      server = TRUE
    )
  })

  shiny::observe({
    if (input$base_procedimentos_ts == "Hospitalares") {
      options_ts <- vars_shiny[names(vars_shiny) == paste0("termos_hosp_", input$ano_ts)] |>
        purrr::flatten() |>
        purrr::flatten_chr()
    } else {
      options_ts <- vars_shiny[names(vars_shiny) == paste0("termos_amb_", input$ano_ts)] |>
        purrr::flatten() |>
        purrr::flatten_chr()
    }

    shiny::updateSelectizeInput(
      session,
      inputId = "procedimento_ts",
      choices = options_ts,
      selected = NULL,
      server = TRUE
    )
  })

  gc()
}

shiny::shinyApp(ui, server)
