resource "docker_network" "retailpulse_network" {
  name     = var.network_name
  driver   = "bridge"
  internal = true
}
