#!/usr/bin/env bash
set -euo pipefail

REPO="${RALPH_REPO:-emperorhan/codex-ralph}"
VERSION="${RALPH_VERSION:-latest}"
INSTALL_DIR="${RALPH_INSTALL_DIR:-}"
VERIFY_SIGNATURE="${RALPH_VERIFY_SIGNATURE:-auto}"

usage() {
  cat <<'EOF'
install-ralphctl.sh

Usage:
  install-ralphctl.sh [--version vX.Y.Z|latest] [--install-dir DIR] [--repo OWNER/REPO]

Env:
  RALPH_VERSION            Release version (default: latest)
  RALPH_INSTALL_DIR        Install directory (default: ~/.local/bin or /usr/local/bin if root)
  RALPH_REPO               GitHub repo (default: emperorhan/codex-ralph)
  RALPH_VERIFY_SIGNATURE   auto|true|false (default: auto)

Examples:
  curl -fsSL https://raw.githubusercontent.com/emperorhan/codex-ralph/main/scripts/install-ralphctl.sh | bash
  RALPH_VERSION=v0.1.0 bash install-ralphctl.sh
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --version)
      VERSION="$2"
      shift 2
      ;;
    --install-dir)
      INSTALL_DIR="$2"
      shift 2
      ;;
    --repo)
      REPO="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

has_cmd() {
  command -v "$1" >/dev/null 2>&1
}

fetch() {
  local url="$1"
  local out="$2"
  if has_cmd curl; then
    curl -fsSL "$url" -o "$out"
    return
  fi
  if has_cmd wget; then
    wget -qO "$out" "$url"
    return
  fi
  echo "curl or wget is required" >&2
  exit 1
}

fetch_stdout() {
  local url="$1"
  if has_cmd curl; then
    curl -fsSL "$url"
    return
  fi
  if has_cmd wget; then
    wget -qO- "$url"
    return
  fi
  echo "curl or wget is required" >&2
  exit 1
}

sha256_file() {
  local file="$1"
  if has_cmd sha256sum; then
    sha256sum "$file" | awk '{print $1}'
    return
  fi
  if has_cmd shasum; then
    shasum -a 256 "$file" | awk '{print $1}'
    return
  fi
  if has_cmd openssl; then
    openssl dgst -sha256 "$file" | awk '{print $NF}'
    return
  fi
  echo "sha256sum/shasum/openssl is required" >&2
  exit 1
}

normalize_os() {
  case "$(uname -s)" in
    Linux) echo "linux" ;;
    Darwin) echo "darwin" ;;
    *)
      echo "unsupported OS: $(uname -s)" >&2
      exit 1
      ;;
  esac
}

normalize_arch() {
  case "$(uname -m)" in
    x86_64|amd64) echo "amd64" ;;
    arm64|aarch64) echo "arm64" ;;
    *)
      echo "unsupported architecture: $(uname -m)" >&2
      exit 1
      ;;
  esac
}

resolve_latest_version() {
  local api="https://api.github.com/repos/${REPO}/releases/latest"
  local raw
  raw="$(fetch_stdout "${api}")"
  local tag
  tag="$(printf "%s\n" "$raw" | sed -n 's/.*"tag_name":[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)"
  if [ -z "${tag}" ]; then
    echo "failed to resolve latest release from ${api}" >&2
    exit 1
  fi
  echo "$tag"
}

if [ "${VERSION}" = "latest" ]; then
  VERSION="$(resolve_latest_version)"
fi

if [ -z "${INSTALL_DIR}" ]; then
  if [ "$(id -u)" -eq 0 ]; then
    INSTALL_DIR="/usr/local/bin"
  else
    INSTALL_DIR="${HOME}/.local/bin"
  fi
fi

OS="$(normalize_os)"
ARCH="$(normalize_arch)"
ASSET="ralphctl_${VERSION}_${OS}_${ARCH}.tar.gz"
BASE_URL="https://github.com/${REPO}/releases/download/${VERSION}"
ASSET_URL="${BASE_URL}/${ASSET}"
CHECKSUMS_URL="${BASE_URL}/checksums.txt"
SIG_URL="${BASE_URL}/checksums.txt.sig"
PUBKEY_URL="${BASE_URL}/cosign.pub"

TMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

echo "[ralphctl-installer] repo=${REPO}"
echo "[ralphctl-installer] version=${VERSION}"
echo "[ralphctl-installer] target=${OS}/${ARCH}"
echo "[ralphctl-installer] install_dir=${INSTALL_DIR}"

fetch "${ASSET_URL}" "${TMP_DIR}/${ASSET}"
fetch "${CHECKSUMS_URL}" "${TMP_DIR}/checksums.txt"

EXPECTED_SHA="$(awk -v f="${ASSET}" '$2==f {print $1}' "${TMP_DIR}/checksums.txt")"
if [ -z "${EXPECTED_SHA}" ]; then
  echo "failed to find checksum for ${ASSET}" >&2
  exit 1
fi
ACTUAL_SHA="$(sha256_file "${TMP_DIR}/${ASSET}")"
if [ "${EXPECTED_SHA}" != "${ACTUAL_SHA}" ]; then
  echo "checksum mismatch for ${ASSET}" >&2
  echo "expected: ${EXPECTED_SHA}" >&2
  echo "actual:   ${ACTUAL_SHA}" >&2
  exit 1
fi
echo "[ralphctl-installer] checksum verified"

SIG_AVAILABLE=false
if fetch "${SIG_URL}" "${TMP_DIR}/checksums.txt.sig" 2>/dev/null && fetch "${PUBKEY_URL}" "${TMP_DIR}/cosign.pub" 2>/dev/null; then
  SIG_AVAILABLE=true
fi

if [ "${VERIFY_SIGNATURE}" = "true" ] || { [ "${VERIFY_SIGNATURE}" = "auto" ] && [ "${SIG_AVAILABLE}" = "true" ]; }; then
  if ! has_cmd cosign; then
    echo "cosign is required for signature verification" >&2
    exit 1
  fi
  if [ "${SIG_AVAILABLE}" != "true" ]; then
    echo "release signature files not found (checksums.txt.sig, cosign.pub)" >&2
    exit 1
  fi
  cosign verify-blob --key "${TMP_DIR}/cosign.pub" --signature "${TMP_DIR}/checksums.txt.sig" "${TMP_DIR}/checksums.txt" >/dev/null
  echo "[ralphctl-installer] signature verified"
elif [ "${VERIFY_SIGNATURE}" = "false" ]; then
  echo "[ralphctl-installer] signature verification skipped"
else
  echo "[ralphctl-installer] signature files not present; checksum verification only"
fi

tar -xzf "${TMP_DIR}/${ASSET}" -C "${TMP_DIR}"
EXTRACTED_BIN="${TMP_DIR}/ralphctl_${VERSION}_${OS}_${ARCH}/ralphctl"
if [ ! -f "${EXTRACTED_BIN}" ]; then
  echo "binary not found in archive: ${EXTRACTED_BIN}" >&2
  exit 1
fi

mkdir -p "${INSTALL_DIR}"
install -m 0755 "${EXTRACTED_BIN}" "${INSTALL_DIR}/ralphctl"
echo "[ralphctl-installer] installed: ${INSTALL_DIR}/ralphctl"

if ! has_cmd ralphctl && ! printf "%s" "${PATH}" | tr ':' '\n' | grep -qx "${INSTALL_DIR}"; then
  echo "[ralphctl-installer] add to PATH: export PATH=\"${INSTALL_DIR}:\$PATH\""
fi

"${INSTALL_DIR}/ralphctl" --help >/dev/null 2>&1 || true
echo "[ralphctl-installer] done"
