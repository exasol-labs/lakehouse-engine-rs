#!/usr/bin/env bash
#   sudo deploy/scripts/install-prereqs.sh
# Docker is only checked, not installed: it needs daemon + group setup.
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "Run with sudo: sudo $0" >&2
  exit 1
fi

REAL_USER="${SUDO_USER:-$(id -un)}"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
have() { command -v "$1" >/dev/null 2>&1; }

log "Updating apt and installing base packages (jq, curl, unzip, openssh-client)..."
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq jq curl unzip ca-certificates gnupg openssh-client

if have tofu; then
  log "OpenTofu already installed: $(tofu version | head -1)"
else
  log "Installing OpenTofu (deb method)..."
  tmp_installer="$(mktemp)"
  curl -fsSL https://get.opentofu.org/install-opentofu.sh -o "$tmp_installer"
  chmod +x "$tmp_installer"
  "$tmp_installer" --install-method deb
  rm -f "$tmp_installer"
  log "OpenTofu installed: $(tofu version | head -1)"
fi

if have aws && aws --version 2>&1 | grep -q 'aws-cli/2'; then
  log "AWS CLI v2 already installed: $(aws --version 2>&1)"
else
  log "Installing AWS CLI v2..."
  arch="$(uname -m)"
  case "$arch" in
    x86_64)  url="https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" ;;
    aarch64) url="https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" ;;
    *) echo "Unsupported arch for AWS CLI v2: $arch" >&2; exit 1 ;;
  esac
  tmp_dir="$(mktemp -d)"
  curl -fsSL "$url" -o "$tmp_dir/awscliv2.zip"
  unzip -q -o "$tmp_dir/awscliv2.zip" -d "$tmp_dir"
  "$tmp_dir/aws/install" --update
  rm -rf "$tmp_dir"
  log "AWS CLI v2 installed: $(aws --version 2>&1)"
fi

if have docker; then
  log "Docker present: $(docker --version)"
else
  log "Docker NOT found. Needed for 'make cross-udf-build' (.so) and bench docker mode."
  log "  Install via https://docs.docker.com/engine/install/ and add '$REAL_USER' to the docker group."
fi

echo
log "Prerequisites installed. Verify versions:"
echo "    tofu version | head -1"
echo "    aws --version"
echo "    jq --version"
echo
log "Next: configure the deployer IAM credentials (created in the IAM setup step):"
echo "    aws configure            # as user '$REAL_USER', NOT root"
echo "    aws sts get-caller-identity"
