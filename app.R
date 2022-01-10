#### SHINY - PROCEDIMENTOS HOSPITALARES ####

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
  geobr,
  sf,
  MetBrewer,
  install = F
)

options(scipen = 999)

# variáveis ---------------------------------------------------------------

hosp_db <- list(
  # ------------------------------------------------------------- #
  `Faixa Etária` = readr::read_csv(here::here("output/base_hosp_idade.csv")) |>
    dplyr::rename(
      `Quantidade total` = tot_qt,
      `Valor total` = tot_vl,
      `Valor médio` = mean_vl
    ) |>
    dplyr::mutate(
      categoria = factor(
        categoria,
        levels = c(
          "< 1",
          "1 a 4",
          "5 a 9",
          "10 a 14",
          "15 a 19",
          "20 a 29",
          "30 a 39",
          "40 a 49",
          "50 a 59",
          "60 a 69",
          "70 a 79",
          "80 <",
          "N. I."
        ) # ordenando categorias de faixa etária
      )
    ) |>
    dplyr::arrange(categoria),
  # ------------------------------------------------------------- #
  Sexo = readr::read_csv(here::here("output/base_hosp_sexo.csv")) |>
    dplyr::rename(
      `Quantidade total` = tot_qt,
      `Valor total` = tot_vl,
      `Valor médio` = mean_vl
    ),
  # ------------------------------------------------------------- #
  UF = readr::read_csv(here::here("output/base_hosp_uf.csv")) |>
    dplyr::rename(
      `Quantidade total` = tot_qt,
      `Valor total` = tot_vl,
      `Valor médio` = mean_vl
    ),
  # ------------------------------------------------------------- #
  procedimentos = readr::read_csv(here::here("output/termos.csv")) |>
    purrr::flatten_chr(),
  # ------------------------------------------------------------- #
  categoria = c("Faixa etária", "Sexo", "UF"),
  # ------------------------------------------------------------- #
  estatistica = c("Quantidade total", "Valor total", "Valor médio")
)

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
            inputId = "procedimentos",
            label = "Selecione um procedimento",
            selected = NULL,
            choices = NULL
          ),
          shiny::selectInput(
            inputId = "categoria",
            label = "Selecione uma categoria",
            selected = NULL,
            choices = hosp_db$categoria
          ),
          shiny::selectInput(
            inputId = "estatistica",
            label = "Selecione uma estatística",
            selected = NULL,
            choices = hosp_db$estatistica
          )
        ),
        # gráficos ------------------------------ #
        shiny::conditionalPanel(
          condition = "input.estatistica == 'Quantidade total' & input.categoria == 'UF'",
          shinydashboard::box(
            plotly::plotlyOutput("db_map"),
            width = 6,
            align = "center"
          )
        ) # mostra mapa apenas para quando a categoria "UF" é selecionada
      ),
      shiny::fluidRow(
        shinydashboard::box(
          plotly::plotlyOutput("db_bar"),
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

server <- function(input, output, session) {
  output$db_bar <- plotly::renderPlotly({
    if (input$categoria == "Sexo") {
      dados <- hosp_db$Sexo
    } else if (input$categoria == "UF") {
      dados <- hosp_db$UF
    } else {
      dados <- hosp_db$`Faixa Etária`
    }

    dados <- dados |>
      dplyr::filter(termo == glue::glue("{input$procedimentos}")) |>
      dplyr::select(
        c(categoria, starts_with(glue::glue("{input$estatistica}")))
      )

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

  output$db_map <- plotly::renderPlotly({
    dados <- hosp_db$UF

    # download de shapefiles de estados
    # geobr::read_state(
    #   code_state = "all",
    #   showProgress = FALSE
    # ) |>
    #   dplyr::select(code_state, abbrev_state, geom) |>
    #   dplyr::rename(categoria = abbrev_state) |>
    #   saveRDS(file = "output/geom_ufs.rds")

    ufs <- readRDS(here::here("output/geom_ufs.rds"))

    plot_map <- dados |>
      dplyr::filter(termo == glue::glue("{input$procedimentos}")) |>
      dplyr::select(
        c(categoria, starts_with(glue::glue("{input$estatistica}")))
      ) |>
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

  shiny::updateSelectizeInput(
    session,
    inputId = "procedimentos",
    choices = hosp_db$procedimentos,
    server = TRUE
  ) # opções server-side
}

shiny::shinyApp(ui, server)
