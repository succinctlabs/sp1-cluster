# AMI for the e2e CI runners (.github/workflows/e2e.yml), both the CPU build job
# and the GPU suite jobs. Bakes everything those jobs otherwise install per run:
# NVIDIA driver 565, CUDA toolkit 12.8, Go, protoc, rustup (nightly), mold + wild
# linkers, cmake, docker (+ nvidia-container-toolkit).
#
# Build (needs AWS credentials for account 421253708207, region us-east-1):
#
#   packer init  infra/packer/e2e-runner.pkr.hcl
#   packer build infra/packer/e2e-runner.pkr.hcl
#
# then put the AMI id it prints into the `ami=` labels in .github/workflows/e2e.yml.
# The workflow's driver / CUDA-library install steps are written as guards, so they
# no-op once the AMI provides them.
#
# Notes:
# - GitHub Actions steps run non-login shells and never source /etc/profile.d; the
#   PATH/RUSTUP_HOME/LD_LIBRARY_PATH the jobs see come from /etc/environment (the
#   same mechanism GitHub's hosted images use). profile.d is only for interactive
#   debugging over SSH/SSM.
# - The driver's kernel module is dkms-built against the kernel that is current at
#   bake time; the template upgrades and reboots first so that kernel is also the
#   one the AMI boots.
# - rustup lives system-wide (RUSTUP_HOME=/usr/local/rustup) because the CI runner
#   user doesn't exist at bake time. The dir is left world-writable so jobs can
#   install the repo's pinned toolchain on top; single-tenant ephemeral runners,
#   so that's acceptable.

packer {
  required_plugins {
    amazon = {
      source  = "github.com/hashicorp/amazon"
      version = ">= 1.3.0"
    }
  }
}

variable "region" {
  type    = string
  default = "us-east-1"
}

# Cheapest GPU instance: lets the bake verify the driver end-to-end (modprobe +
# nvidia-smi). The driver supports both its T4 and the g6 fleet's L4.
variable "instance_type" {
  type    = string
  default = "g4dn.xlarge"
}

# Same VPC/subnet the previous runner AMI (github-runner-ami-*) was built in.
variable "vpc_id" {
  type    = string
  default = "vpc-0a2ca18ab8ea85a21"
}

variable "subnet_id" {
  type    = string
  default = "subnet-0e6be174427425552"
}

variable "root_volume_size" {
  type    = number
  default = 50
}

variable "go_version" {
  type    = string
  default = "1.22.1" # matches what the workflow's setup-go pinned
}

variable "protoc_version" {
  type    = string
  default = "29.4"
}

variable "mold_version" {
  type    = string
  default = "2.41.0"
}

variable "wild_version" {
  type    = string
  default = "0.9.0"
}

locals {
  timestamp = regex_replace(timestamp(), "[- TZ:]", "")
}

source "amazon-ebs" "e2e_runner" {
  region        = var.region
  instance_type = var.instance_type

  source_ami_filter {
    filters = {
      virtualization-type = "hvm"
      name                = "ubuntu/images/*ubuntu-jammy-22.04-amd64-server-*"
      root-device-type    = "ebs"
    }
    owners      = ["099720109477"] # Canonical
    most_recent = true
  }

  ssh_username                = "ubuntu"
  associate_public_ip_address = true
  vpc_id                      = var.vpc_id
  subnet_id                   = var.subnet_id

  ami_name        = "sp1-cluster-e2e-runner-${local.timestamp}"
  ami_description = "sp1-cluster e2e runner: NVIDIA driver 565, CUDA 12.8, Go ${var.go_version}, protoc, rustup nightly, mold+wild, cmake, docker"

  tags = {
    Name = "sp1-cluster e2e runner"
    Repo = "succinctlabs/sp1-cluster"
  }

  launch_block_device_mappings {
    device_name           = "/dev/sda1"
    volume_size           = var.root_volume_size
    volume_type           = "gp3"
    delete_on_termination = true
  }
}

build {
  sources = ["source.amazon-ebs.e2e_runner"]

  # Base system + the apt set the e2e build job needs (infra/Dockerfile.node_gpu's
  # dependency list, minus CUDA which gets its own step).
  provisioner "shell" {
    environment_vars = ["DEBIAN_FRONTEND=noninteractive"]
    inline = [
      # First boot races cloud-init: it is still writing /etc/apt/sources.list when
      # packer connects, so an early apt-get update fetches nothing and every install
      # after it fails with "no installation candidate". Wait it out (|| true: a
      # degraded-but-done boot is fine for a bake — anything real fails loudly below).
      "cloud-init status --wait || true",
      "sudo apt-get update",
      "sudo apt-get upgrade -y",
      "sudo apt-get install -y --no-install-recommends openssl libssl-dev pkg-config build-essential libclang-dev python3-pip diffutils gcc m4 make libnvtoolsext1 wget tar unzip git curl openssh-client ca-certificates gnupg linux-headers-generic",
    ]
  }

  # Boot the upgraded kernel before the driver install so dkms targets the kernel
  # the AMI will actually run.
  provisioner "shell" {
    expect_disconnect = true
    inline            = ["sudo reboot"]
  }

  provisioner "shell" {
    pause_before     = "15s"
    environment_vars = ["DEBIAN_FRONTEND=noninteractive"]
    inline = [
      # Same cloud-init race applies to the post-reboot boot.
      "cloud-init status --wait || true",
      # docker (upstream repo, same set the previous runner AMI carried)
      "sudo install -m 0755 -d /etc/apt/keyrings",
      "sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc",
      "sudo chmod a+r /etc/apt/keyrings/docker.asc",
      "echo \"deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable\" | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null",
      "sudo apt-get update",
      "sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin",
      # nvidia-container-toolkit so GPU containers keep working on this AMI
      "curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg",
      "curl -fsSL https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list >/dev/null",
      "sudo apt-get update",
      "sudo apt-get install -y nvidia-container-toolkit",
      "sudo nvidia-ctk runtime configure --runtime=docker",
      "sudo systemctl restart docker",
    ]
  }

  # CUDA toolkit 12.8 (12.8.1 is the final point release the -12-8 metapackage
  # resolves to) + the 565 driver branch it is paired with in CI. Verified live:
  # this instance type has a GPU, so a broken driver fails the bake, not a CI run.
  provisioner "shell" {
    environment_vars = ["DEBIAN_FRONTEND=noninteractive"]
    inline = [
      "wget -q https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/cuda-keyring_1.1-1_all.deb",
      "sudo dpkg -i cuda-keyring_1.1-1_all.deb && rm cuda-keyring_1.1-1_all.deb",
      "sudo apt-get update",
      "sudo apt-get install -y cuda-toolkit-12-8 nvidia-driver-565",
      "sudo modprobe nvidia",
      "nvidia-smi",
      "/usr/local/cuda/bin/nvcc --version",
    ]
  }

  # Go, protoc, cmake, mold, wild — all version-pinned, all on the default PATH so
  # no job-side PATH surgery is needed.
  provisioner "shell" {
    inline = [
      # go
      "curl -sSfL https://go.dev/dl/go${var.go_version}.linux-amd64.tar.gz -o /tmp/go.tgz",
      "sudo tar -C /usr/local -xzf /tmp/go.tgz && rm /tmp/go.tgz",
      "sudo ln -sf /usr/local/go/bin/go /usr/local/go/bin/gofmt /usr/local/bin/",
      # protoc
      "curl -sSfL -o /tmp/protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v${var.protoc_version}/protoc-${var.protoc_version}-linux-x86_64.zip",
      "sudo unzip -o /tmp/protoc.zip -d /usr/local bin/protoc 'include/*' && rm /tmp/protoc.zip",
      # cmake >= 3.24 (ubuntu22's apt ships 3.22; pip carries current releases —
      # the same route the workflow used, proven against sp1-gpu-sys/sppark)
      "sudo pip3 install --upgrade cmake",
      # mold (bin/mold + libexec/mold/ld for the cc -B shim)
      "curl -sSfL https://github.com/rui314/mold/releases/download/v${var.mold_version}/mold-${var.mold_version}-x86_64-linux.tar.gz | sudo tar -xz --strip-components=1 -C /usr/local",
      # wild (static musl build; shim dir mirrors mold's so -B works with GCC)
      "curl -sSfL https://github.com/davidlattimore/wild/releases/download/${var.wild_version}/wild-linker-${var.wild_version}-x86_64-unknown-linux-musl.tar.gz | sudo tar -xz --strip-components=1 -C /usr/local/bin --wildcards '*/wild'",
      "sudo mkdir -p /usr/local/libexec/wild",
      "sudo ln -sf /usr/local/bin/wild /usr/local/libexec/wild/ld",
    ]
  }

  # rustup, system-wide, with the latest nightly. The repo's rust-toolchain.toml
  # pins its own nightly; rustup fetches that on first use, so the baked one only
  # saves the rustup bootstrap itself and serves as a fallback toolchain.
  provisioner "shell" {
    inline = [
      "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sudo RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo sh -s -- -y --no-modify-path --default-toolchain nightly --profile minimal --component llvm-tools --component rustc-dev",
      "sudo ln -sf /usr/local/cargo/bin/* /usr/local/bin/",
      "sudo chmod -R a+rwX /usr/local/rustup /usr/local/cargo",
    ]
  }

  # Environment for CI jobs (/etc/environment) and for interactive shells
  # (profile.d). PATH in /etc/environment is a literal, not expanded.
  provisioner "shell" {
    inline = [
      "sudo sed -i 's#^PATH=\"#PATH=\"/usr/local/cuda/bin:#' /etc/environment",
      "echo 'LD_LIBRARY_PATH=\"/usr/local/cuda/lib64\"' | sudo tee -a /etc/environment",
      "echo 'RUSTUP_HOME=\"/usr/local/rustup\"' | sudo tee -a /etc/environment",
      "printf 'export PATH=/usr/local/cuda/bin:$PATH\\nexport LD_LIBRARY_PATH=/usr/local/cuda/lib64:$LD_LIBRARY_PATH\\nexport RUSTUP_HOME=/usr/local/rustup\\n' | sudo tee /etc/profile.d/e2e-runner.sh",
    ]
  }

  # Everything the e2e jobs assume, verified in one place so a bad bake fails here.
  provisioner "shell" {
    inline = [
      "set -x",
      "nvidia-smi --query-gpu=driver_version --format=csv,noheader",
      "/usr/local/cuda/bin/nvcc --version | tail -1",
      "ldconfig -p | grep -q libnvrtc.so.12",
      "go version",
      "protoc --version",
      "cmake --version | head -1",
      "mold --version",
      "wild --version",
      "RUSTUP_HOME=/usr/local/rustup rustc --version",
      "sudo docker run --rm hello-world",
    ]
  }

  # Snapshot hygiene: a clone must not reuse instance state or machine identity.
  provisioner "shell" {
    inline = [
      "sudo apt-get clean",
      "sudo rm -rf /var/lib/apt/lists/*",
      "sudo cloud-init clean --logs",
      "sudo truncate -s 0 /etc/machine-id",
      "sudo rm -f /var/lib/dbus/machine-id",
    ]
  }
}
