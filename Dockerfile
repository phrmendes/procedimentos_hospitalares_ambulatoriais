# base image https://hub.docker.com/u/rocker/
# docker build -t phrmendes12/proc_hosp:10_01_22 .

FROM rocker/shiny-verse:latest

RUN apt update -y && apt upgrade -y && apt clean -y && apt install -y  libudunits2-dev libgdal-dev libgeos-dev libproj-dev

RUN mkdir /home/shiny/output

COPY app.R /home/shiny

COPY output/* /home/shiny/output

RUN Rscript -e 'install.packages(c("shiny", "pacman", "glue", "usethis", "shinydashboard", "plotly", "shinydashboardPlus", "geobr", "sf", "MetBrewer", "here"))'

# expose port

EXPOSE 3838

# run app on container start

 CMD ["Rscript", "-e", "shiny::runApp('/home/shiny', host = '0.0.0.0', port = 3838)"]

# docker run -d --rm -p 3838:3838 phrmendes12/proc_hosp:10_01_22
