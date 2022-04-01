# ---------------------------------------------------------- #
# --- SHINY - PROCEDIMENTOS HOSPITALARES E AMBULATORIAIS --- #
# ---------------------------------------------------------- #

# bibliotecas e parâmetros ------------------------------------------------

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
library(showtext)
library(writexl)

options(scipen = 999)
options(dplyr.summarise.inform = FALSE)
sysfonts::font_add_google("Fira Sans")

# variáveis ---------------------------------------------------------------

geom_ufs <- readr::read_rds("output/geom_ufs.rds")

termos <- arrow::open_dataset("output/db_termos_shiny/")

db <- arrow::open_dataset("output/db_shiny/")

vars_shiny <- list(
  categoria = c("Faixa etária", "Sexo", "UF"),
  estatistica = c("Quantidade total", "Valor total", "Valor médio")
)

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
  disable = TRUE,
  collapsed = TRUE,
  shinydashboard::sidebarMenu()
)

# body --------------------------------------------------------------------

# style = "font-size:14px;font-family:'Fira Sans', sans-serif;", # fonte open sans


body <- shinydashboard::dashboardBody(
  tags$head(
    tags$link(rel = "stylesheet", type = "text/css", href = "bootstrap_custom.css")
  ),
  h2("Procedimentos Médicos e Ambulatoriais", style = "font-family:'Fira Sans', sans-serif;"),
  shiny::fluidRow(
    width = 12,
    column(
      width = 2,
      align = "center",
      shiny::selectInput(
        inputId = "base_procedimentos",
        label = "Base de procedimentos",
        selected = NULL,
        choices = c("Hospitalares", "Ambulatoriais")
      )
    ),
    column(
      width = 2,
      align = "center",
      shiny::selectInput(
        inputId = "ano",
        label = "Ano",
        selected = NULL,
        choices = 2018:2020
      )
    ),
    column(
      width = 4,
      align = "center",
      shiny::selectizeInput(
        inputId = "procedimento",
        label = "Procedimento",
        selected = NULL,
        choices = NULL
      )
    ),
    column(
      width = 2,
      align = "center",
      shiny::selectInput(
        inputId = "categoria",
        label = "Categoria",
        selected = NULL,
        choices = vars_shiny$categoria
      )
    ),
    column(
      width = 2,
      align = "center",
      shiny::selectInput(
        inputId = "estatistica",
        label = "Selecione uma estatística",
        selected = NULL,
        choices = vars_shiny$estatistica
      )
    )#,
    # column(
    #   width = 2,
    #   align = "center",
    #   shiny::actionButton(
    #     inputId = "botao",
    #     label = "Busca",
    #     icon = shiny::icon("fa-circle-dot")
    #   )
    # )
  ),
  shiny::fluidRow(
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
  ),
  shiny::fluidRow(
    shinydashboard::box(
      shinycssloaders::withSpinner(plotly::plotlyOutput("bar")),
      width = 6,
      align = "center"
    ),
    shinydashboard::box(
      shinycssloaders::withSpinner(plotly::plotlyOutput("ts")),
      width = 6,
      align = "center"
    )
  ),
  shiny::fluidRow(
    shiny::conditionalPanel(
      condition = "input.categoria == 'UF'",
      shinydashboard::box(
        shinycssloaders::withSpinner(plotly::plotlyOutput("map")),
        width = 4,
        align = "center"
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
#   categoria = "Faixa Etária",
#   estatistica = "Quantidade total",
#   procedimento = "CONSULTA EM CONSULTÓRIO (NO HORÁRIO NORMAL OU PREESTABELECIDO)",
#   ano = "2018"
# )

server <- function(input, output, session) {
  dados_anuais <- shiny::reactive({
    if (input$base_procedimentos == "Hospitalares") {
      dados_anuais <- db |>
        dplyr::filter(db == "hosp")
    } else {
      dados_anuais <- db |>
        dplyr::filter(db == "amb")
    }

    estatistica <- as.symbol(input$estatistica)

    periodo <- paste0("1-", 1:12, "-", input$ano)

    dados_anuais <- dados_anuais |>
      dplyr::filter(
        tipo == stringr::str_to_lower(input$categoria),
        termo == input$procedimento,
        mes_ano %in% periodo
      ) |>
      dplyr::rename(
        `Quantidade total` = tot_qt,
        `Valor total` = tot_vl,
        `Valor médio` = mean_vl
      ) |>
      dplyr::group_by(cd_procedimento, termo, categoria) |>
      dplyr::summarise({{ estatistica }} := sum({{ estatistica }})) |>
      dplyr::ungroup() |>
      dplyr::select(categoria, {{ estatistica }}) |>
      dplyr::collect()

    if (input$categoria == "Faixa Etária") {
      dados_anuais <- dados_anuais |>
        dplyr::mutate(
          categoria = factor(
            categoria,
            levels = c("< 1", "1 a 4", "5 a 9", "10 a 14", "15 a 19", "20 a 29", "30 a 39", "40 a 49", "50 a 59", "60 a 69", "70 a 79", "80 <", "N. I.")
          )
        ) |>
        dplyr::arrange(categoria)
    }

    return(dados_anuais)
  })

  dados_mensais <- shiny::reactive({
    if (input$base_procedimentos == "Hospitalares") {
      dados_mensais <- db |>
        dplyr::filter(db == "hosp")
    } else {
      dados_mensais <- db |>
        dplyr::filter(db == "amb")
    }

    periodo <- paste0("1-", 1:12, "-", input$ano)

    estatistica <- as.symbol(input$estatistica)

    dados_mensais <- dados_mensais |>
      dplyr::filter(
        tipo == stringr::str_to_lower(input$categoria),
        termo == input$procedimento,
        mes_ano %in% periodo
      ) |>
      dplyr::rename(
        `Quantidade total` = tot_qt,
        `Valor total` = tot_vl,
        `Valor médio` = mean_vl
      ) |>
      dplyr::group_by(cd_procedimento, termo, categoria, mes_ano) |>
      dplyr::summarise({{ estatistica }} := sum({{ estatistica }})) |>
      dplyr::ungroup() |>
      dplyr::select(categoria, mes_ano, {{ estatistica }}) |>
      dplyr::collect()

    if (input$categoria == "Faixa Etária") {
      dados_anuais <- dados_anuais |>
        dplyr::mutate(
          categoria = factor(
            categoria,
            levels = c("< 1", "1 a 4", "5 a 9", "10 a 14", "15 a 19", "20 a 29", "30 a 39", "40 a 49", "50 a 59", "60 a 69", "70 a 79", "80 <", "N. I.")
          )
        ) |>
        dplyr::arrange(categoria)
    }

    return(dados_mensais)
  })

  output$bar <- plotly::renderPlotly({
    plot_bar <- dados_anuais() |>
      dplyr::arrange(categoria) |>
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
          glue::glue("{input$estatistica} de procedimentos por {input$categoria} ({input$ano})")
        )
      ) +
      theme_minimal() +
      xlab(glue::glue("{input$categoria}")) +
      theme(
        legend.position = "none",
        text = element_text(
          size = 13,
          family = "Fira Sans"
        ),
        plot.title = element_text(
          face = "bold",
          hjust = 0.5
        )
      ) +
      scale_fill_manual(values = MetBrewer::met.brewer(
        name = "Hokusai2",
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
          family = "Fira Sans"
        ),
        plot.title = element_text(
          face = "bold",
          hjust = 0.5
        )
      ) +
      ggtitle(
        stringr::str_to_upper(
          glue::glue(
            "{input$estatistica} de procedimentos por {input$categoria} ({input$ano})"
          )
        )
      )

    plotly::ggplotly(plot_map)
  })

  output$ts <- plotly::renderPlotly({
    plot_line <- dados_mensais() |>
      dplyr::mutate(
        mes_ano = lubridate::dmy(mes_ano),
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
      labs(
        x = "",
        y = input$estatistica,
        color = input$categoria
      ) +
      theme_minimal() +
      theme(
        legend.position = "right",
        legend.title = element_blank(),
        text = element_text(
          size = 13,
          family = "Fira Sans"
        ),
        plot.title = element_text(
          face = "bold",
          hjust = 0.5
        )
      ) +
      scale_color_manual(values = MetBrewer::met.brewer(
        name = "Hokusai3",
        n = 27,
        type = "continuous"
      )) +
      ggtitle(
        stringr::str_to_upper(
          glue::glue(
            "{input$estatistica} de procedimentos por {input$categoria} (jan - dez/{input$ano})"
          )
        )
      )

    plotly::ggplotly(plot_line)
  })

  output$download_anual <- shiny::downloadHandler(
    filename = function() {
      glue::glue("dados_{janitor::make_clean_names(input$base_procedimentos)}_{janitor::make_clean_names(input$categoria)}_{janitor::make_clean_names(input$estatistica)}_{input$ano}.xlsx")
    },
    content = function(file) {
      writexl::write_xlsx(dados_anuais(), file)
    }
  )

  output$download_mensal <- shiny::downloadHandler(
    filename = function() {
      glue::glue("dados_mensais_{janitor::make_clean_names(input$base_procedimentos)}_{janitor::make_clean_names(input$categoria)}_{janitor::make_clean_names(input$estatistica)}_{input$ano}.xlsx")
    },
    content = function(file) {
      writexl::write_xlsx(dados_mensais(), file)
    }
  )

  # opções server-side

  shiny::observe({
    if (input$base_procedimentos == "Hospitalares") {
      options <- termos |>
        dplyr::filter(db == "hosp")
    } else {
      options <- termos |>
        dplyr::filter(db == "amb")
    }

    periodo <- paste0("1-1-", input$ano)

    options <- options |>
      dplyr::filter(ano == periodo) |>
      dplyr::select(termos) |>
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

  gc()
}

shiny::shinyApp(ui, server)
