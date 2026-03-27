terraform {
  required_providers {
    oci = {
      source  = "oracle/oci"
      version = "~> 6.0"
    }
  }
}

provider "oci" {
  tenancy_ocid     = var.tenancy_ocid
  user_ocid        = var.user_ocid
  fingerprint      = var.fingerprint
  private_key_path = var.private_key_path
  region           = var.region
}

# ─── VCN ─────────────────────────────────────────────────────────────────────

resource "oci_core_vcn" "reptilian" {
  compartment_id = var.compartment_ocid
  cidr_block     = "10.0.0.0/16"
  display_name   = "reptilian-vcn"
  dns_label      = "reptilian"
}

resource "oci_core_internet_gateway" "reptilian" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.reptilian.id
  display_name   = "reptilian-igw"
  enabled        = true
}

resource "oci_core_route_table" "reptilian" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.reptilian.id
  display_name   = "reptilian-rt"

  route_rules {
    destination       = "0.0.0.0/0"
    network_entity_id = oci_core_internet_gateway.reptilian.id
  }
}

resource "oci_core_security_list" "reptilian" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.reptilian.id
  display_name   = "reptilian-sl"

  # Allow all outbound
  egress_security_rules {
    destination = "0.0.0.0/0"
    protocol    = "all"
  }

  # SSH
  ingress_security_rules {
    protocol = "6" # TCP
    source   = "0.0.0.0/0"
    tcp_options {
      min = 22
      max = 22
    }
  }

  # HTTP
  ingress_security_rules {
    protocol = "6"
    source   = "0.0.0.0/0"
    tcp_options {
      min = 80
      max = 80
    }
  }

  # HTTPS
  ingress_security_rules {
    protocol = "6"
    source   = "0.0.0.0/0"
    tcp_options {
      min = 443
      max = 443
    }
  }

  # Coolify UI (8000)
  ingress_security_rules {
    protocol = "6"
    source   = "0.0.0.0/0"
    tcp_options {
      min = 8000
      max = 8000
    }
  }

  # Prefect UI (4200)
  ingress_security_rules {
    protocol = "6"
    source   = var.your_ip_cidr
    tcp_options {
      min = 4200
      max = 4200
    }
  }

  # Mission Control (3001)
  ingress_security_rules {
    protocol = "6"
    source   = var.your_ip_cidr
    tcp_options {
      min = 3001
      max = 3001
    }
  }

  # Second Brain API (8001)
  ingress_security_rules {
    protocol = "6"
    source   = var.your_ip_cidr
    tcp_options {
      min = 8001
      max = 8001
    }
  }
}

resource "oci_core_subnet" "reptilian" {
  compartment_id    = var.compartment_ocid
  vcn_id            = oci_core_vcn.reptilian.id
  cidr_block        = "10.0.1.0/24"
  display_name      = "reptilian-subnet"
  dns_label         = "reptilian"
  route_table_id    = oci_core_route_table.reptilian.id
  security_list_ids = [oci_core_security_list.reptilian.id]
}

# ─── Instance ────────────────────────────────────────────────────────────────

data "oci_core_images" "ubuntu_arm" {
  compartment_id           = var.compartment_ocid
  operating_system         = "Canonical Ubuntu"
  operating_system_version = "22.04"
  shape                    = "VM.Standard.A1.Flex"
  sort_by                  = "TIMECREATED"
  sort_order               = "DESC"
}

resource "oci_core_instance" "reptilian" {
  compartment_id      = var.compartment_ocid
  availability_domain = var.availability_domain
  display_name        = "reptilian-main"
  shape               = "VM.Standard.A1.Flex"

  shape_config {
    ocpus         = 4
    memory_in_gbs = 24
  }

  source_details {
    source_type             = "image"
    source_id               = data.oci_core_images.ubuntu_arm.images[0].id
    boot_volume_size_in_gbs = 100
  }

  create_vnic_details {
    subnet_id        = oci_core_subnet.reptilian.id
    assign_public_ip = true
    display_name     = "reptilian-vnic"
    hostname_label   = "reptilian"
  }

  metadata = {
    ssh_authorized_keys = var.ssh_public_key
    user_data           = base64encode(file("${path.module}/cloud-init.sh"))
  }
}

# ─── Outputs ─────────────────────────────────────────────────────────────────

output "public_ip" {
  value = oci_core_instance.reptilian.public_ip
}

output "ssh_command" {
  value = "ssh ubuntu@${oci_core_instance.reptilian.public_ip}"
}
