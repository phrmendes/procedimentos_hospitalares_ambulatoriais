#### SHINY - PROCEDIMENTOS HOSPITALARES ####

# bibliotecas, funções e opções -------------------------------------------

source(here::here("R/bibliotecas.R"))

options(scipen = 999)

# variáveis ---------------------------------------------------------------

hosp_db <- list(
  `Faixa Etária` = readr::read_csv(here::here("output/base_hosp_idade.csv")) |>
    dplyr::rename(
      `Quantidade total` = tot_qt,
      `Valor total` = tot_vl,
      `Valor médio` = mean_vl
    ) |>
    dplyr::mutate(
      categoria = dplyr::case_when(
        categoria == "<1" ~ "< 1",
        categoria == "80 ou mais" ~ "80 <",
        TRUE ~ as.character(categoria)
      ),
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
          "Não identificado"
        ) # ordenando categorias de faixa etária
      )
    ) |>
    dplyr::arrange(categoria),
  Sexo = readr::read_csv(here::here("output/base_hosp_sexo.csv")) |>
    dplyr::mutate(categoria = dplyr::case_when(
      is.na(categoria) ~ "Não informado",
      TRUE ~ as.character(categoria)
    )) |>
    dplyr::rename(
      `Quantidade total` = tot_qt,
      `Valor total` = tot_vl,
      `Valor médio` = mean_vl
    ),
  UF = readr::read_csv(here::here("output/base_hosp_uf.csv")) |>
    dplyr::rename(
      `Quantidade total` = tot_qt,
      `Valor total` = tot_vl,
      `Valor médio` = mean_vl
    ),
  procedimentos = readr::read_csv(here::here("output/termos.csv")) |>
    purrr::flatten_chr(),
  categoria = c("Faixa etária", "Sexo", "UF"),
  estatistica = c("Quantidade total", "Valor total", "Valor médio")
)

# shiny -------------------------------------------------------------------

header <- shinydashboard::dashboardHeader(
  title = "Procedimentos Hospitalares"
)

sidebar <- shinydashboard::dashboardSidebar(
  shinydashboard::sidebarMenu(
    shinydashboard::menuItem(
      "Procedimentos Hospitalares",
      tabName = "hosp",
      icon = icon("chart-bar")
    )
  )
) # menu lateral do dashboard

body <- shinydashboard::dashboardBody(
  shinydashboard::tabItems(
    shinydashboard::tabItem(
      tabName = "hosp",
      h2("Procedimentos Hospitalares"),
      shiny::fluidRow(
        shinydashboard::box(
          title = "Parâmetros",
          width = 2,
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
        ), # caixas de seleção de parâmetros
        shinydashboard::box(
          plotly::plotlyOutput("db"),
          height = 420,
          width = 10,
          align = "center"
        ), # gráficos
      )
    )
  )
)

ui <- shinydashboard::dashboardPage(header, sidebar, body)

server <- function(input, output, session) {
  output$db <- plotly::renderPlotly({
    if (input$categoria == "Sexo") {
      dados <- hosp_db$Sexo
    } else if (input$categoria == "UF") {
      dados <- hosp_db$UF
    } else {
      dados <- hosp_db$`Faixa Etária`
    } 

    dados <- dados |>
      dplyr::filter(termo == glue::glue("{input$procedimentos}")) |>
      dplyr::select(c(categoria, starts_with(glue::glue("{input$estatistica}"))))

    plot <- dados |>
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
      xlab(glue::glue("{input$categoria}")) +
      theme(legend.position = "none") +
      scale_fill_manual(values = MetBrewer::met.brewer(
        name = "Ingres",
        n = 26,
        type = "continuous"
      ))

    plotly::ggplotly(plot)
  })

  shiny::updateSelectizeInput(
    session,
    inputId = "procedimentos",
    choices = hosp_db$procedimentos,
    server = TRUE
  ) # opções server-side
}

shiny::shinyApp(ui, server)
