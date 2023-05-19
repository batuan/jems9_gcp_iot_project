provider "google" {
#   credentials = "/Users/batuan/Documents/apply_viec_2023/jems/DataScientest/data-iot-poei-project-321b743606a5.json"
  project     = "data-iot-poei-project"
  region      = "europe-west9"
}

resource "google_compute_network" "emqx_network" {
  name = "emqx-network"
}

resource "google_compute_subnetwork" "emqx_subnetwork" {
  name          = "emqx-subnetwork"
  ip_cidr_range = "10.0.0.0/24"
  network       = google_compute_network.emqx_network.self_link
}

resource "google_compute_firewall" "emqx_firewall" {
  name    = "emqx-firewall"
  network = google_compute_network.emqx_network.self_link

  allow {
    protocol = "tcp"
    ports    = ["1883", "8883", "8083", "8084", "22", "80", "443", "18083"]  # Adjust the ports according to your EMQ X configuration
  }

  source_ranges = ["0.0.0.0/0"]
}

resource "google_compute_firewall" "emqx_ping" {
  name    = "emqx-ping"
  network = google_compute_network.emqx_network.self_link

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]  # Adjust the ports according to your EMQ X configuration
  }
  allow {
    protocol = "udp"
    ports = ["0-65535"]
  }

  allow {
    protocol = "icmp"
  }

  allow {
    protocol = "sctp"
  }

  source_ranges = [
    "10.0.0.0/24",
  ]
}

resource "google_compute_instance" "emqx_ee" {
  count        = 3
  name         = "emqx-ee-${count.index}"
  machine_type = "e2-medium"
  zone         = "europe-west9-a"

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2004-lts"
      size = 20
    }
  }

  metadata = {
    ssh-keys = "thaibatuana1k41pbc:${file("/Users/batuan/.ssh/id_rsa.pub")}"
  }

  network_interface {
    network       = google_compute_network.emqx_network.self_link
    subnetwork    = google_compute_subnetwork.emqx_subnetwork.self_link
    access_config {
      // Leave this empty to assign an ephemeral IP address to each instance
    }
    network_ip = "10.0.0.1${count.index}"
  }
  
  connection {
    type        = "ssh"
    user        = "thaibatuana1k41pbc"
    private_key = file("/Users/batuan/.ssh/id_rsa")
    host        = self.network_interface[0].access_config[0].nat_ip
  }
  
  provisioner "remote-exec" {
    inline = [
      "sudo apt-get update",
      "sudo apt-get upgrade -y",
      "wget https://www.emqx.com/en/downloads/enterprise/5.0.3/emqx-enterprise-5.0.3-ubuntu20.04-amd64.tar.gz",
      "mkdir -p emqx && tar -zxvf emqx-enterprise-5.0.3-ubuntu20.04-amd64.tar.gz -C emqx",
      "echo '${file("./template.conf")}' > ./emqx/etc/emqx.conf",
      "echo 'node.name = 'emqx@${self.network_interface[0].network_ip}'' >> ./emqx/etc/emqx.conf",
      "./emqx/bin/emqx start"
    ]
  }

}

output "external_ips" {
  value = google_compute_instance.emqx_ee[*].network_interface.0.access_config.0.nat_ip
}

output "instance_ips" {
  value = google_compute_instance.emqx_ee[*].network_interface[0].network_ip
}
