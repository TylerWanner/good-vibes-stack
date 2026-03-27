variable "tenancy_ocid" {
  description = "OCID of your tenancy"
  type        = string
}

variable "user_ocid" {
  description = "OCID of the user running Terraform"
  type        = string
}

variable "fingerprint" {
  description = "Fingerprint of the API key"
  type        = string
}

variable "private_key_path" {
  description = "Path to the OCI API private key (PEM)"
  type        = string
  default     = "~/.oci/oci_api_key.pem"
}

variable "region" {
  description = "OCI region"
  type        = string
  default     = "us-ashburn-1"
}

variable "compartment_ocid" {
  description = "OCID of the compartment (use tenancy OCID for root)"
  type        = string
}

variable "availability_domain" {
  description = "Availability domain (e.g. unja:US-ASHBURN-AD-1)"
  type        = string
}

variable "ssh_public_key" {
  description = "SSH public key for instance access"
  type        = string
}

variable "your_ip_cidr" {
  description = "Your IP in CIDR notation for restricting sensitive ports (e.g. 1.2.3.4/32)"
  type        = string
  default     = "0.0.0.0/0"
}
