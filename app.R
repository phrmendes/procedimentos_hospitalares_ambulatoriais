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
      width = 3,
      "proc"
    ),
    bs4Dash::infoBoxOutput(
      width = 3,
      "qtd_tot"
    ),
    bs4Dash::infoBoxOutput(
      width = 3,
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

  # option_procedimento = "ANATOMIA PATOLÓGICA E CITOPATOLOGIA"
  # option_ano = "2019"
  # option_estatistica = "Quantidade total"
  # option_categoria = "UF Prestador"
  # option_db = "amb"
  # infoboxes_db = "amb"

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
      input$ano
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

    periodo <- paste0("1-", 1:12, "-", option_ano())

    dados_anuais <- db |>
      dplyr::filter(
        db == option_db(),
        tipo == janitor::make_clean_names(option_categoria()),
        termo == option_procedimento(),
        mes_ano %in% periodo
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
      dplyr::ungroup() |>
      dplyr::collect()

    return(dados_anuais)
  })

  # dados mensais ---------------------------------------------------------

  dados_mensais <- shiny::reactive({
    periodo <- paste0("1-", 1:12, "-", option_ano())

    estatistica <- as.symbol(option_estatistica())

    dados_mensais <- db |>
      dplyr::filter(
        db == option_db(),
        tipo == janitor::make_clean_names(option_categoria()),
        termo == option_procedimento(),
        mes_ano %in% periodo
      ) |>
      dplyr::rename(
        `Quantidade total` = tot_qt,
        `Valor total` = tot_vl,
        `Valor médio` = mean_vl
      ) |>
      dplyr::group_by(categoria, mes_ano)

    if (option_estatistica() %in% c(vars_shiny$estatistica[1:2])) {
      dados_mensais <- dados_mensais |>
        dplyr::summarise({{ estatistica }} := sum({{ estatistica }}))
    } else {
      dados_mensais <- dados_mensais |>
        dplyr::summarise({{ estatistica }} := mean({{ estatistica }}))
    }

    if (option_categoria() == "UF Prestador") {
      dados_mensais <- dados_mensais |>
        arrow::to_duckdb() |>
        dplyr::mutate(
          categoria = dplyr::case_when(
            categoria %in% c("AC", "AM", "AP", "PA", "RO", "RR", "TO") ~ "Norte",
            categoria %in% c("AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE") ~ "Nordeste",
            categoria %in% c("DF", "GO", "MS", "MT") ~ "Centro-Oeste",
            categoria %in% c("RS", "SC", "PR") ~ "Sul",
            TRUE ~ "Sudeste"
          )
        ) |>
        dplyr::group_by(categoria, mes_ano)

      if (option_estatistica() %in% c(vars_shiny$estatistica[1:2])) {
        dados_mensais <- dados_mensais |>
          dplyr::summarise({{ estatistica }} := sum(
            {{ estatistica }},
            na.rm = TRUE
          ))
      } else {
        dados_mensais <- dados_mensais |>
          dplyr::summarise(
            {{ estatistica }} := mean(
              {{ estatistica }},
              na.rm = TRUE
            )
          )
      }
    }

    dados_mensais <- dados_mensais |>
      dplyr::collect()

    return(dados_mensais)
  })

  # info boxes ------------------------------------------------------------

  output$proc <- bs4Dash::renderInfoBox({
    periodo <- paste0("1-1-", input$ano)

    n <- termos |>
      dplyr::filter(ano == periodo & db == infoboxes_db()) |>
      dplyr::summarise(n = n()) |>
      dplyr::collect() |>
      dplyr::pull()

    bs4Dash::infoBox(
      title = shiny::HTML("Nº de procedimentos disponíveis para consulta:"),
      value = prettyNum(n, big.mark = "\\."),
      icon = shiny::icon("notes-medical", lib = "font-awesome"),
      color = "primary"
    )
  })

  output$qtd_tot <- bs4Dash::renderInfoBox({
    periodo <- paste0("1-", 1:12, "-", input$ano)

    qtd_tot <- db |>
      dplyr::filter(
        mes_ano %in% periodo,
        db == infoboxes_db(),
        tipo == "sexo"
      ) |>
      dplyr::summarise(tot_qt = sum(tot_qt)) |>
      dplyr::collect() |>
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
      title = shiny::HTML("Quantidade de procedimentos realizados durante o ano:"),
      value = qtd_tot,
      icon = shiny::icon("calendar", lib = "font-awesome"),
      color = "orange"
    )
  })

  output$vl_tot <- bs4Dash::renderInfoBox({
    periodo <- paste0("1-", 1:12, "-", input$ano)

    vl_tot <- db |>
      dplyr::filter(
        mes_ano %in% periodo,
        db == infoboxes_db(),
        tipo == "sexo"
      ) |>
      dplyr::summarise(vl_tot = sum(tot_vl)) |>
      dplyr::collect() |>
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
      title = shiny::HTML("Valor total gasto em procedimentos no ano:"),
      value = vl_tot,
      icon = shiny::icon("coins", lib = "font-awesome"),
      color = "lightblue"
    )
  })

  # bar plot --------------------------------------------------------------

  output$bar <- plotly::renderPlotly({
    estatistica <- as.symbol(option_estatistica())

    if (option_categoria() == "Faixa etária") {
      plot_bar <- dados_anuais() |>
        dplyr::mutate(
          categoria = forcats::as_factor(categoria),
          categoria = forcats::fct_relevel(
            categoria,
            c("< 1", "1 a 4", "5 a 9", "10 a 14", "15 a 19", "20 a 29", "30 a 39", "40 a 49", "50 a 59", "60 a 69", "70 a 79", "80 <", "N. I.")
          )
        )
    } else {
      plot_bar <- dados_anuais() |>
        dplyr::mutate(
          categoria = forcats::as_factor(categoria),
          categoria := forcats::fct_reorder(categoria, {{ estatistica }})
        )
    }

    plot_bar <- plot_bar |>
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

  # time series per region plot -------------------------------------------

  output$ts <- plotly::renderPlotly({
    if (option_categoria() == "Faixa etária") {
      plot_line <- dados_mensais() |>
        dplyr::mutate(
          categoria = forcats::as_factor(categoria),
          categoria = forcats::fct_relevel(
            categoria,
            c("< 1", "1 a 4", "5 a 9", "10 a 14", "15 a 19", "20 a 29", "30 a 39", "40 a 49", "50 a 59", "60 a 69", "70 a 79", "80 <", "N. I.")
          )
        )
    } else {
      plot_line <- dados_mensais() |>
        dplyr::mutate(categoria = forcats::as_factor(categoria))
    }

    plot_line <- dados_mensais() |>
      dplyr::mutate(mes_ano = lubridate::dmy(mes_ano)) |>
      ggplot() +
      geom_line(
        aes_string(
          x = names(dados_mensais())[2],
          y = glue::glue("`{names(dados_mensais())[3]}`"),
          group = names(dados_mensais())[1],
          color = names(dados_mensais())[1]
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
        legend.title = element_blank(),
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
            "{option_estatistica()} do procedimento por região (jan - dez/{option_ano()})"
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
}

shiny::shinyApp(ui, server)
