############################################
#### SHINY - PROCEDIMENTOS HOSPITALARES ####
############################################

# bibliotecas, funções e opções -------------------------------------------

if (!require("pacman")) install.packages("pacman")

library("pacman")

pacman::p_load(
  tidyverse,
  glue,
  usethis,
  shinydashboard,
  plotly,
  shinydashboardPlus,
  here,
  MetBrewer,
  shiny,
  sf,
  DBI,
  duckdb,
  install = F
)

# install.packages("sf", configure.args = "--with-proj-lib=/usr/local/lib/")

options(scipen = 999)

# variáveis ---------------------------------------------------------------

shinydb <- duckdb::dbConnect(
  duckdb::duckdb(),
  dbdir = "output/shinydb.duckdb"
)

selectize_vars <- list(
  proc_hosp = dplyr::tbl(shinydb, "termos_hosp") |>
    dplyr::collect() |>
    purrr::flatten_chr(),
  proc_amb = dplyr::tbl(shinydb, "termos_amb") |>
    dplyr::collect() |>
    purrr::flatten_chr(),
  categoria = c("Faixa etária", "Sexo", "UF"),
  estatistica = c("Quantidade total", "Valor total", "Valor médio")
)

duckdb::dbDisconnect(shinydb, shutdown = TRUE)

ufs <- readRDS(here::here("output/geom_ufs.rds"))

# shiny -------------------------------------------------------------------

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

# menu lateral do dashboard ---------------- #
sidebar <- shinydashboard::dashboardSidebar(
  shinydashboard::sidebarMenu(
    shinydashboard::menuItem(
      "Procedimentos Hospitalares",
      tabName = "hosp",
      icon = icon("chart-bar")
    ),
    shinydashboard::menuItem(
      "Procedimentos Ambulatoriais",
      tabName = "amb",
      icon = icon("chart-bar")
    )
  )
)

body <- shinydashboard::dashboardBody(
  shinydashboard::tabItems(
    # procedimentos hospitalares
    shinydashboard::tabItem(
      # seleção de parâmetros ----------------- #
      tabName = "hosp",
      h2("Procedimentos Hospitalares"),
      shiny::fluidRow(
        shinydashboard::box(
          title = "Parâmetros",
          width = 6,
          solidHeader = TRUE,
          collapsible = TRUE,
          shiny::selectizeInput(
            inputId = "procedimentos_hosp",
            label = "Selecione um procedimento",
            selected = NULL,
            choices = NULL
          ),
          shiny::selectInput(
            inputId = "categoria",
            label = "Selecione uma categoria",
            selected = NULL,
            choices = selectize_vars$categoria
          ),
          shiny::selectInput(
            inputId = "estatistica",
            label = "Selecione uma estatística",
            selected = NULL,
            choices = selectize_vars$estatistica
          )
        ),
        # gráficos ------------------------------ #
        shiny::conditionalPanel(
          condition = "input.estatistica == 'Quantidade total' & input.categoria == 'UF'",
          shinydashboard::box(
            plotly::plotlyOutput("hosp_map"),
            width = 6,
            align = "center"
          )
        ) # mostra mapa apenas para quando a categoria "UF" é selecionada
      ),
      shiny::fluidRow(
        shinydashboard::box(
          plotly::plotlyOutput("hosp_bar"),
          width = 12,
          align = "center"
        )
      )
    ),
    # procedimentos ambulatoriais
    shinydashboard::tabItem(
      # seleção de parâmetros ----------------- #
      tabName = "amb",
      h2("Procedimentos Ambulatoriais"),
      shiny::fluidRow(
        shinydashboard::box(
          title = "Parâmetros",
          width = 6,
          solidHeader = TRUE,
          collapsible = TRUE,
          shiny::selectizeInput(
            inputId = "procedimentos_amb",
            label = "Selecione um procedimento",
            selected = NULL,
            choices = NULL
          ),
          shiny::selectInput(
            inputId = "categoria",
            label = "Selecione uma categoria",
            selected = NULL,
            choices = selectize_vars$categoria
          ),
          shiny::selectInput(
            inputId = "estatistica",
            label = "Selecione uma estatística",
            selected = NULL,
            choices = selectize_vars$estatistica
          )
        ),
        # gráficos ------------------------------ #
        shiny::conditionalPanel(
          condition = "input.estatistica == 'Quantidade total' & input.categoria == 'UF'",
          shinydashboard::box(
            plotly::plotlyOutput("amb_map"),
            width = 6,
            align = "center"
          )
        ) # mostra mapa apenas para quando a categoria "UF" e a estatística "Quantidade total são selecionadas
      ),
      shiny::fluidRow(
        shinydashboard::box(
          plotly::plotlyOutput("amb_bar"),
          width = 12,
          align = "center"
        )
      )
    )
  )
)

ui <- shinydashboard::dashboardPage(
  title = "Abramge", # título da aba do dashboard
  header, sidebar, body
)

input <- list(
  categoria = "UF",
  procedimentos = "CONSULTA EM PRONTO SOCORRO",
  estatistica = "Quantidade total"
)

server <- function(input, output, session) {

  # procedimentos hospitalares ----

  output$hosp_bar <- plotly::renderPlotly({
    shinydb <- duckdb::dbConnect(
      duckdb::duckdb(),
      dbdir = "output/shinydb.duckdb"
    )

    if (input$categoria == "Sexo") {
      dados <- dplyr::tbl(shinydb, "base_hosp_sexo") |>
        dplyr::collect()
    } else if (input$categoria == "UF") {
      dados <- dplyr::tbl(shinydb, "base_hosp_uf") |>
        dplyr::collect()
    } else {
      dados <- dplyr::tbl(shinydb, "base_hosp_idade") |>
        dplyr::collect()
    }

    duckdb::dbDisconnect(shinydb, shutdown = TRUE)

    dados <- dados |>
      dplyr::filter(termo == input$procedimentos) |>
      dplyr::select(categoria, dplyr::starts_with(glue::glue("{input$estatistica}")))

    plot_bar <- dados |>
      ggplot() +
      geom_col(
        aes_string(
          x = names(dados)[1],
          y = glue::glue("`{names(dados)[2]}`"),
          fill = names(dados)[1]
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
  }) # gráfico de barras

  output$hosp_map <- plotly::renderPlotly({
    shinydb <- duckdb::dbConnect(
      duckdb::duckdb(),
      dbdir = "output/shinydb.duckdb"
    )

    dados <- dplyr::tbl(shinydb, "base_hosp_uf") |>
      dplyr::collect()

    duckdb::dbDisconnect(shinydb, shutdown = TRUE)

    plot_map <- dados |>
      dplyr::filter(termo == glue::glue("{input$procedimentos}")) |>
      dplyr::select(categoria, tot_qt) |>
      dplyr::rename(`Quantidade total` = tot_qt) |>
      dplyr::left_join(
        ufs,
        by = "categoria"
      ) |>
      # dplyr::filter(termo == "ANESTESIAS") |>
      ggplot() +
      geom_sf(
        aes(
          geometry = geom,
          fill = `Quantidade total`
        ),
        color = NA
      ) +
      theme_minimal() +
      scale_fill_gradientn(colors = MetBrewer::met.brewer("Ingres")) +
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
  }) # mapa

  # procedimentos ambulatoriais ----

  output$amb_bar <- plotly::renderPlotly({
    shinydb <- duckdb::dbConnect(
      duckdb::duckdb(),
      dbdir = "output/shinydb.duckdb"
    )

    if (input$categoria == "Sexo") {
      dados <- dplyr::tbl(shinydb, "base_amb_sexo") |>
        dplyr::collect()
    } else if (input$categoria == "UF") {
      dados <- dplyr::tbl(shinydb, "base_amb_uf") |>
        dplyr::collect()
    } else {
      dados <- dplyr::tbl(shinydb, "base_amb_idade") |>
        dplyr::collect()
    }

    duckdb::dbDisconnect(shinydb, shutdown = TRUE)

    dados <- dados |>
      dplyr::filter(termo == input$procedimentos) |>
      dplyr::select(categoria, dplyr::starts_with(glue::glue("{input$estatistica}")))

    plot_bar <- dados |>
      ggplot() +
      geom_col(
        aes_string(
          x = names(dados)[1],
          y = glue::glue("`{names(dados)[2]}`"),
          fill = names(dados)[1]
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
  }) # gráfico de barras

  output$amb_map <- plotly::renderPlotly({
    shinydb <- duckdb::dbConnect(
      duckdb::duckdb(),
      dbdir = "output/shinydb.duckdb"
    )

    dados <- dplyr::tbl(shinydb, "base_amb_uf") |>
      dplyr::collect()

    duckdb::dbDisconnect(shinydb, shutdown = TRUE)

    plot_map <- dados |>
      dplyr::filter(termo == glue::glue("{input$procedimentos}")) |>
      dplyr::select(categoria, tot_qt) |>
      dplyr::rename(`Quantidade total` = tot_qt) |>
      dplyr::left_join(
        ufs,
        by = "categoria"
      ) |>
      # dplyr::filter(termo == "ANESTESIAS") |>
      ggplot() +
      geom_sf(
        aes(
          geometry = geom,
          fill = `Quantidade total`
        ),
        color = NA
      ) +
      theme_minimal() +
      scale_fill_gradientn(colors = MetBrewer::met.brewer("Ingres")) +
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
  }) # mapa

  # opções server-side

  shiny::updateSelectizeInput(
    session,
    inputId = "procedimentos_hosp",
    choices = selectize_vars$proc_hosp,
    server = TRUE
  )

  shiny::updateSelectizeInput(
    session,
    inputId = "procedimentos_amb",
    choices = selectize_vars$proc_amb,
    server = TRUE
  ) # opções server-side
}

shiny::shinyApp(ui, server)
