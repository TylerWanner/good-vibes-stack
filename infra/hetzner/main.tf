terraform {
  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "~> 1.50"
    }
  }
}

provider "hcloud" {
  token = var.hcloud_token
}

resource "hcloud_ssh_key" "reptilian" {
  name       = "reptilian"
  public_key = var.ssh_public_key
}

resource "hcloud_server" "reptilian" {
  name        = "reptilian-main"
  server_type = "cax31"          # 8 ARM vCPU, 16GB RAM — enough for qwen3.5:9b + full stack
  image       = "ubuntu-22.04"
  location    = "nbg1"           # Nuremberg; alternatives: hel1 (Helsinki), fsn1 (Falkenstein)
  ssh_keys    = [hcloud_ssh_key.reptilian.id]
  user_data   = file("${path.module}/cloud-init.sh")

  labels = {
    project = "reptilian"
  }
}

resource "hcloud_firewall" "reptilian" {
  name = "reptilian-fw"

  # SSH — open (harden with fail2ban post-provision)
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "22"
    source_ips = ["0.0.0.0/0", "::/0"]
  }

  # HTTP / HTTPS — open for Coolify + any public services
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "80"
    source_ips = ["0.0.0.0/0", "::/0"]
  }

  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "443"
    source_ips = ["0.0.0.0/0", "::/0"]
  }

  # Coolify UI — restricted to your IP
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "8000"
    source_ips = [var.your_ip_cidr]
  }

  # Prefect UI — restricted to your IP
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "4200"
    source_ips = [var.your_ip_cidr]
  }

  # Second Brain API — restricted to your IP
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "8001"
    source_ips = [var.your_ip_cidr]
  }
}

resource "hcloud_firewall_attachment" "reptilian" {
  firewall_id = hcloud_firewall.reptilian.id
  server_ids  = [hcloud_server.reptilian.id]
}

# ─── Outputs ──────────────────────────────────────────────────────────────────

output "public_ip" {
  value = hcloud_server.reptilian.ipv4_address
}

output "ssh_command" {
  value = "ssh root@${hcloud_server.reptilian.ipv4_address}"
}
