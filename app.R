# ---------------------------------------------------------- #
# --- SHINY - PROCEDIMENTOS HOSPITALARES E AMBULATORIAIS --- #
# ---------------------------------------------------------- #

# bibliotecas, funções e opções -------------------------------------------

library(tidyverse)
library(glue)
library(shinydashboard)
# library(shinydashboardPlus)
# library(dashboardthemes)
library(plotly)
library(MetBrewer)
library(shiny)
library(sf)
library(arrow)
library(tmaptools)

options(scipen = 999)

# reactlog::reactlog_enable()

# variáveis ---------------------------------------------------------------

vars_shiny <- list(
  proc_hosp = arrow::read_parquet("output/termos_hosp_2020.parquet") |>
    dplyr::arrange(termos) |>
    purrr::flatten_chr(),
  proc_amb = arrow::read_parquet("output/termos_amb_2020.parquet") |>
    dplyr::arrange(termos) |>
    purrr::flatten_chr(),
  categoria = c("Faixa etária", "Sexo", "UF"),
  estatistica = c("Quantidade total", "Valor total", "Valor médio")
)

shinydb <- purrr::map(
  fs::dir_ls("output/", regexp = "base(.*)parquet"),
  arrow::read_parquet
)

names(shinydb) <- names(shinydb) |>
  stringr::str_extract("(?<=output/)(.*)(?=.parquet)")

for (i in 1:length(shinydb)) {
  names(shinydb[[i]])[4:6] <- vars_shiny$estatistica
}

# levels = c("< 1", "1 a 4", "5 a 9", "10 a 14", "15 a 19", "20 a 29", "30 a 39", "40 a 49", "50 a 59", "60 a 69", "70 a 79", "80 <", "N. I.")

ufs <- readr::read_rds("output/geom_ufs.rds")

# header ------------------------------------------------------------------

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
      tabName = "proc",
      h2("Lista de procedimentos"),
      shiny::fluidRow(
        shinydashboard::box(
          title = "Parâmetros",
          width = 6,
          solidHeader = TRUE,
          collapsible = TRUE,
          shiny::selectInput(
            inputId = "base_procedimentos",
            label = "Selecione uma base de procedimentos",
            selected = NULL,
            choices = c("Hospitalares", "Ambulatoriais")
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
            plotly::plotlyOutput("map"),
            width = 6,
            align = "center"
          )
        )
      ),
      shiny::fluidRow(
        shinydashboard::box(
          plotly::plotlyOutput("bar"),
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

server <- function(input, output, session) {

  dados <- shiny::reactive(
    if (input$base_procedimentos == "Hospitalares") {
      if (input$categoria == "Sexo") {
        dados <- shinydb$base_hosp_sexo
      } else if (input$categoria == "UF") {
        dados <- shinydb$base_hosp_uf
      } else {
        dados <- shinydb$base_hosp_idade
      }
    } else {
      if (input$categoria == "Sexo") {
        dados <- shinydb$base_amb_sexo
      } else if (input$categoria == "UF") {
        dados <- shinydb$base_amb_uf
      } else {
        dados <- shinydb$base_amb_idade
      }
    }
  )

  output$bar <- plotly::renderPlotly({

    df <- dados() |>
      dplyr::filter(termo == input$procedimento) |>
      dplyr::select(categoria, dplyr::starts_with(glue::glue("{input$estatistica}")))

    plot_bar <- df |>
      ggplot() +
      geom_col(
        aes_string(
          x = names(df)[1],
          y = glue::glue("`{names(df)[2]}`"),
          fill = names(df)[1]
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
    plot_map <- dados() |>
      dplyr::filter(termo == input$procedimento) |>
      dplyr::select(categoria, dplyr::starts_with(glue::glue("{input$estatistica}"))) |>
      dplyr::left_join(
        ufs,
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

  # opções server-side

  reactive_selectisize <- shiny::reactive(
    if (input$base_procedimentos == "Hospitalar") {
      return(vars_shiny$proc_hosp)
    } else {
      return(vars_shiny$proc_amb)
    }
  )

  shiny::observe({
    if (input$base_procedimentos == "Hospitalar") {
      options <- vars_shiny$proc_hosp
    } else {
      options <- vars_shiny$proc_amb
    }

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

# shiny::reactlogShow()

