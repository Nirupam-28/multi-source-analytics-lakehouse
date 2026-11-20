output "docker_network_name" {
  description = "Name of the Terraform-managed Docker network"
  value       = docker_network.retailpulse_network.name
}

output "docker_network_id" {
  description = "ID of the Terraform-managed Docker network"
  value       = docker_network.retailpulse_network.id
}
