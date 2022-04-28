pacman::p_load(googleComputeEngineR, install = TRUE)

vm <- googleComputeEngineR::gce_vm(
  name = "dashboard-vm",
  predefined_type = "n1-standard-8",
  template = "rstudio",
  username = "phrmendes",
  password = "01001011"
)

googleComputeEngineR::gce_vm_start("dashboard-vm")
googleComputeEngineR::gce_vm_stop("dashboard-vm")
googleComputeEngineR::gce_vm_delete("dashboard-vm")
