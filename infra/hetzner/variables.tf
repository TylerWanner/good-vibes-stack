variable "hcloud_token" {
  description = "Hetzner Cloud API token — generate at console.hetzner.cloud → Security → API Tokens (Read & Write)"
  sensitive   = true
}

variable "ssh_public_key" {
  description = "SSH public key contents (e.g. contents of ~/.ssh/id_ed25519.pub)"
}

variable "your_ip_cidr" {
  description = "Your IP in CIDR notation for restricted ports (e.g. 1.2.3.4/32)"
}
