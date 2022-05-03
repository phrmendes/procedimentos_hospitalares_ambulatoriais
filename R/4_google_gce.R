pacman::p_load(googleComputeEngineR, install = FALSE)

vm <- googleComputeEngineR::gce_vm(
  name = "dashboard-vm",
  predefined_type = "n1-standard-16",
  template = "rstudio",
  username = "phrmendes",
  password = "01001011",
  disk_size_gb = 200
)

googleComputeEngineR::gce_vm_start("dashboard-vm")
googleComputeEngineR::gce_vm_stop("dashboard-vm")
googleComputeEngineR::gce_vm_delete("dashboard-vm")
