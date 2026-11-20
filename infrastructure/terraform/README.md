
# Terraform – Infrastructure as Code

## 1. Overview

This directory contains Terraform configuration for managing Docker infrastructure used by the **Multi-Source Analytics Lakehouse (RetailPulse Analytics)** project.

Terraform is used to provision and manage infrastructure declaratively using Infrastructure as Code (IaC).

The project uses the Docker Terraform provider to create and manage Docker resources.

## 2. Technology Stack

- Terraform
- Docker Engine
- Docker Terraform Provider
- HashiCorp Configuration Language (HCL)

## 3. Infrastructure Managed by Terraform

Terraform currently manages the following resource:

| Resource Type | Resource Name | Purpose |
|---|---|---|
| Docker Network | `retailpulse_terraform_network` | Dedicated isolated Docker bridge network |

### Network Configuration

| Property | Value |
|---|---|
| Network name | `retailpulse_terraform_network` |
| Driver | `bridge` |
| Internal | `true` |
| Scope | `local` |

**Architecture decision:** The primary lakehouse infrastructure, including PostgreSQL, SILO object storage, Nessie, Trino, Spark, and Airflow, is managed by Docker Compose.

Terraform manages a separate Docker network to demonstrate declarative infrastructure provisioning without creating conflicting ownership of existing Docker resources.

## 4. Project Structure

```text
terraform/
├── main.tf
├── providers.tf
├── variables.tf
├── outputs.tf
├── terraform.tfvars.example
├── README.md
└── .gitignore
```

| File | Description |
|---|---|
| `providers.tf` | Defines Terraform version requirements and Docker provider configuration |
| `main.tf` | Defines Docker infrastructure resources |
| `variables.tf` | Declares configurable input variables |
| `outputs.tf` | Exposes useful infrastructure information |
| `terraform.tfvars.example` | Provides example variable values |
| `README.md` | Documents Terraform infrastructure and deployment instructions |

Terraform also generates local files and directories such as `.terraform/` and `terraform.tfstate`. These should not be committed to Git.

## 5. Prerequisites

Ensure the following tools are installed:

- Docker Engine with Docker daemon running
- Terraform version 1.5 or later
- Access to the Docker socket

Verify installations:

```bash
docker --version
terraform version
```

## 6. Terraform Configuration

### Provider Configuration

The project uses the Docker provider:

```hcl
terraform {
  required_version = ">= 1.5.0"

  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.6"
    }
  }
}

provider "docker" {
  host = "unix:///var/run/docker.sock"
}
```

### Input Variables

The Docker network name is configurable through a Terraform variable:

```hcl
variable "network_name" {
  description = "Name of the Terraform-managed Docker network"
  type        = string
  default     = "retailpulse_terraform_network"
}
```

### Docker Network Resource

```hcl
resource "docker_network" "retailpulse_network" {
  name     = var.network_name
  driver   = "bridge"
  internal = true
}
```

The `internal` property restricts external connectivity for containers attached to this network.

### Terraform Outputs

Terraform exposes the network name and generated Docker network ID:

```hcl
output "docker_network_name" {
  description = "Name of the Terraform-managed Docker network"
  value       = docker_network.retailpulse_network.name
}

output "docker_network_id" {
  description = "ID of the Terraform-managed Docker network"
  value       = docker_network.retailpulse_network.id
}
```

## 7. Deployment Instructions

Navigate to the Terraform directory:

```bash
cd infrastructure/terraform
```

### Step 1: Initialize Terraform

Download and initialize the required provider:

```bash
terraform init
```

### Step 2: Format Configuration

Format Terraform configuration files:

```bash
terraform fmt
```

### Step 3: Validate Configuration

Validate the Terraform configuration:

```bash
terraform validate
```

### Step 4: Review Execution Plan

Preview the infrastructure changes:

```bash
terraform plan
```

### Step 5: Apply Infrastructure

Provision the Docker network:

```bash
terraform apply
```

Review the execution plan and confirm by entering `yes`.

## 8. Verification

### Verify Terraform State

List Terraform-managed resources:

```bash
terraform state list
```

Expected resource:

```text
docker_network.retailpulse_network
```

Inspect the resource:

```bash
terraform state show docker_network.retailpulse_network
```

### Verify Docker Network

```bash
docker network inspect retailpulse_terraform_network
```

Verify the internal network setting:

```bash
docker network inspect retailpulse_terraform_network \
  --format '{{.Internal}}'
```

Expected output:

```text
true
```

### View Terraform Outputs

```bash
terraform output
```

This displays the managed Docker network name and ID.

## 9. Infrastructure Change Management

Terraform compares the desired configuration with the deployed infrastructure.

For example, changing the Docker network's `internal` property from `false` to `true` requires replacement because the Docker provider treats this property as immutable.

Terraform identifies this change during planning:

```text
-/+ destroy and then create replacement
```

The execution plan must always be reviewed before applying infrastructure changes.

Useful commands:

```bash
terraform plan
terraform apply
terraform state list
terraform state show docker_network.retailpulse_network
```

## 10. Terraform State Management

Terraform maintains a state file to track the infrastructure it manages.

Local state file:

```text
terraform.tfstate
```

The state file contains resource metadata and infrastructure attributes.

For this local development project, Terraform uses local state.

For production environments, a secured remote backend with state locking, access controls, and appropriate encryption should be configured.

**Important:** Never commit Terraform state files or files containing credentials to a public Git repository.

## 11. Git Security

The following Terraform files and directories should be excluded from Git:

```gitignore
**/.terraform/*
*.tfstate
*.tfstate.*
*.tfvars
!*.tfvars.example
crash.log
crash.*.log
override.tf
override.tf.json
*_override.tf
*_override.tf.json
.terraformrc
terraform.rc
```

This prevents local Terraform state, provider downloads, and potentially sensitive variable files from being committed.

## 12. Key Concepts Demonstrated

Through this module, the project demonstrates:

- Infrastructure as Code using Terraform.
- Docker provider configuration.
- Terraform resources and input variables.
- Terraform outputs.
- Provider initialization.
- Configuration formatting and validation.
- Infrastructure planning and provisioning.
- Terraform state management.
- Infrastructure change detection.
- Resource replacement and lifecycle management.
- Git hygiene and state-file security.

## 13. Future Improvements

Potential improvements for a production deployment include:

- Remote Terraform state storage.
- State locking and versioning.
- Separate development, staging, and production environments.
- Reusable Terraform modules.
- Cloud infrastructure provisioning.
- CI/CD integration for Terraform validation and planning.
- Infrastructure policy and security checks.

---

**Module Status:** Completed

**Project:** Multi-Source Analytics Lakehouse

**Infrastructure as Code:** Terraform + Docker
