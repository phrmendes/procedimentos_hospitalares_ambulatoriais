# base image https://hub.docker.com/u/rocker/
# docker build . -t pedromendes12/proc_ans:beta_1

FROM rocker/shiny-verse:latest

RUN apt update -y && apt upgrade -y && apt clean -y && apt install -y libudunits2-dev libgdal-dev libgeos-dev libproj-dev unzip

COPY app.R /home/shiny/

RUN mkdir /home/shiny/output

COPY output/ /home/shiny/output

RUN Rscript -e 'install.packages(c("glue", "bs4Dash", "plotly", "MetBrewer", "shinydashboard", "sf", "arrow", "memoise", "tmaptools", "rlang", "shinycssloaders", "sysfonts", "writexl", "geobr"))'

RUN echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen \
   && locale-gen en_US.utf8 \
   && /usr/sbin/update-locale LANG=en_US.UTF-8

# expose port

EXPOSE 3838

# run app on container start

CMD ["Rscript", "-e", "shiny::runApp('/home/shiny', host = '0.0.0.0', port = 3838)"]

# docker run --rm -p 3838:3838 pedromendes12/proc_ans:beta_1
