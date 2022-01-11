# bibliotecas e funções ---------------------------------------------------

library(googleComputeEngineR)

# vm ----------------------------------------------------------------------

vm <- gce_vm(
  name = "vm-proj", # nome da vm
  predefined_type = "n1-standard-8", # VM type
  template = "rstudio", # imagem docker com rstudio
  username = "phrmendes",
  password = "01001011" # username and password RStudio
)

gce_vm_stop("vm-proj")

gce_vm_delete("vm-proj")
