#!/usr/bin/env bash
set -euo pipefail

APP_NAME="sheaflive"
SERVICE_USER="sheaflive"
DOMAIN=""
REPO_URL=""
REPO_REF="main"
ADMIN_USER="${SUDO_USER:-}"
ADMIN_PUBLIC_KEY=""
ADMIN_PUBLIC_KEY_FILE=""
ADMIN_PASSWORD_HASH=""
GO_VERSION="1.25.10"
GO_SHA256_LINUX_AMD64="42d4f7a32316aa66591eca7e89867256057a4264451aca10570a715b3637ba70"
TIMEZONE="Europe/London"
HARDEN_SSH=0
CONFIGURE_UFW=1
INSTALL_CADDY=1
RUN_INITIAL_INGEST=0

APP_DIR="/opt/${APP_NAME}"
DATA_DIR="/var/lib/${APP_NAME}"
ENV_FILE="/etc/${APP_NAME}.env"
BUILD_DIR=""

usage() {
	cat <<'EOF'
Usage:
  sudo bash scripts/bootstrap-vps.sh --domain DOMAIN --repo-url REPO_URL [options]

Required:
  --domain DOMAIN                 Public domain, for example sheaflive.com.
  --repo-url REPO_URL             Git clone URL for this repository.

Common options:
  --repo-ref REF                  Branch, tag, or commit to deploy. Default: main.
  --admin-password-hash HASH      Existing bcrypt hash for the admin UI.
                                  If omitted, the script prompts for a passphrase.
  --run-ingest-now                Run ingest once during bootstrap.

SSH hardening options:
  --harden-ssh                    Disable root/password SSH login after installing
                                  the admin user's public key.
  --admin-user USER               Human sudo user to create/update. Default: SUDO_USER.
  --ssh-public-key KEY            Public key line to install for --admin-user.
  --ssh-public-key-file PATH      Public key file to install for --admin-user.

Skip options:
  --skip-ufw                      Do not configure the local UFW firewall.
  --skip-caddy                    Do not install or configure Caddy.

Examples:
  sudo bash /tmp/bootstrap-vps.sh \
    --domain sheaflive.com \
    --repo-url https://github.com/OWNER/REPO.git \
    --admin-user nathan \
    --ssh-public-key 'ssh-ed25519 AAAA... comment' \
    --harden-ssh

  sudo bash /tmp/bootstrap-vps.sh \
    --domain sheaflive.com \
    --repo-url https://github.com/OWNER/REPO.git \
    --admin-password-hash '$2a$10$...'
EOF
}

log() {
	printf '[%s] %s\n' "$(date +'%Y-%m-%dT%H:%M:%S%z')" "$*" >&2
}

warn() {
	printf 'warning: %s\n' "$*" >&2
}

die() {
	printf 'error: %s\n' "$*" >&2
	exit 1
}

cleanup() {
	if [[ -n "${BUILD_DIR}" && -d "${BUILD_DIR}" ]]; then
		rm -rf "${BUILD_DIR}"
	fi
}
trap cleanup EXIT

parse_args() {
	while [[ $# -gt 0 ]]; do
		case "$1" in
		--domain)
			DOMAIN="${2:-}"
			shift 2
			;;
		--repo-url)
			REPO_URL="${2:-}"
			shift 2
			;;
		--repo-ref)
			REPO_REF="${2:-}"
			shift 2
			;;
		--admin-password-hash)
			ADMIN_PASSWORD_HASH="${2:-}"
			shift 2
			;;
		--admin-user)
			ADMIN_USER="${2:-}"
			shift 2
			;;
		--ssh-public-key)
			ADMIN_PUBLIC_KEY="${2:-}"
			shift 2
			;;
		--ssh-public-key-file)
			ADMIN_PUBLIC_KEY_FILE="${2:-}"
			shift 2
			;;
		--harden-ssh)
			HARDEN_SSH=1
			shift
			;;
		--skip-ufw)
			CONFIGURE_UFW=0
			shift
			;;
		--skip-caddy)
			INSTALL_CADDY=0
			shift
			;;
		--run-ingest-now)
			RUN_INITIAL_INGEST=1
			shift
			;;
		-h|--help)
			usage
			exit 0
			;;
		*)
			die "unknown argument: $1"
			;;
		esac
	done
}

require_root() {
	if [[ "${EUID}" -ne 0 ]]; then
		die "run this script as root, usually with sudo"
	fi
}

validate_inputs() {
	[[ -n "${DOMAIN}" ]] || die "--domain is required"
	[[ -n "${REPO_URL}" ]] || die "--repo-url is required"
	[[ "${DOMAIN}" =~ ^[A-Za-z0-9.-]+$ ]] || die "--domain contains unsupported characters"
	if [[ "${HARDEN_SSH}" -eq 1 ]]; then
		[[ -n "${ADMIN_USER}" ]] || die "--admin-user is required with --harden-ssh"
		if [[ -z "${ADMIN_PUBLIC_KEY}" && -z "${ADMIN_PUBLIC_KEY_FILE}" ]]; then
			die "--ssh-public-key or --ssh-public-key-file is required with --harden-ssh"
		fi
	fi
}

install_packages() {
	log "Refreshing apt metadata and installing base packages"
	export DEBIAN_FRONTEND=noninteractive
	apt-get update
	apt-get install -y ca-certificates curl gnupg git sqlite3 tar
	if [[ "${HARDEN_SSH}" -eq 1 ]]; then
		apt-get install -y openssh-client
	fi
	if [[ "${CONFIGURE_UFW}" -eq 1 ]]; then
		apt-get install -y ufw
	fi
}

install_go() {
	local arch asset archive checksum current_version
	arch="$(uname -m)"
	case "${arch}" in
	x86_64)
		asset="linux-amd64"
		checksum="${GO_SHA256_LINUX_AMD64}"
		;;
	*)
		die "unsupported architecture for bundled Go install: ${arch}"
		;;
	esac

	if [[ -x /usr/local/go/bin/go ]]; then
		current_version="$(/usr/local/go/bin/go version | awk '{print $3}')"
		if [[ "${current_version}" == "go${GO_VERSION}" ]]; then
			log "Go ${GO_VERSION} is already installed"
			ln -sf /usr/local/go/bin/go /usr/local/bin/go
			ln -sf /usr/local/go/bin/gofmt /usr/local/bin/gofmt
			return
		fi
	fi

	log "Installing Go ${GO_VERSION} for ${asset}"
	archive="go${GO_VERSION}.${asset}.tar.gz"
	pushd /tmp >/dev/null
	curl -fsSLO "https://go.dev/dl/${archive}"
	printf '%s  %s\n' "${checksum}" "${archive}" | sha256sum -c -
	rm -rf /usr/local/go
	tar -C /usr/local -xzf "${archive}"
	popd >/dev/null

	cat >/etc/profile.d/go.sh <<'EOF'
export PATH=/usr/local/go/bin:$PATH
EOF
	chmod 644 /etc/profile.d/go.sh
	ln -sf /usr/local/go/bin/go /usr/local/bin/go
	ln -sf /usr/local/go/bin/gofmt /usr/local/bin/gofmt
}

ensure_service_user_and_dirs() {
	log "Creating service user and persistent directories"
	if ! id -u "${SERVICE_USER}" >/dev/null 2>&1; then
		adduser --system --group --home "${APP_DIR}" --shell /usr/sbin/nologin "${SERVICE_USER}"
	fi

	install -d -o root -g root -m 755 "${APP_DIR}" "${APP_DIR}/bin" "${APP_DIR}/config"
	install -d -o "${SERVICE_USER}" -g "${SERVICE_USER}" -m 750 "${DATA_DIR}" "${DATA_DIR}/media"
}

read_public_key() {
	if [[ -n "${ADMIN_PUBLIC_KEY}" ]]; then
		printf '%s\n' "${ADMIN_PUBLIC_KEY}"
		return
	fi
	[[ -r "${ADMIN_PUBLIC_KEY_FILE}" ]] || die "cannot read SSH public key file: ${ADMIN_PUBLIC_KEY_FILE}"
	sed -n '1p' "${ADMIN_PUBLIC_KEY_FILE}"
}

validate_public_key_line() {
	local public_key="$1"
	local tmp

	[[ -n "${public_key}" ]] || die "SSH public key cannot be empty"
	command -v ssh-keygen >/dev/null 2>&1 || die "ssh-keygen is required to validate SSH public keys"

	tmp="$(mktemp)"
	printf '%s\n' "${public_key}" >"${tmp}"
	if ! ssh-keygen -l -f "${tmp}" >/dev/null 2>&1; then
		rm -f "${tmp}"
		die "SSH public key is not parseable by ssh-keygen"
	fi
	rm -f "${tmp}"
}

ensure_admin_user_and_key() {
	[[ "${HARDEN_SSH}" -eq 1 ]] || return

	local public_key home_dir auth_file
	log "Creating/updating admin user ${ADMIN_USER}"
	if ! id -u "${ADMIN_USER}" >/dev/null 2>&1; then
		adduser --disabled-password --gecos "" "${ADMIN_USER}"
	fi
	usermod -aG sudo "${ADMIN_USER}"

	public_key="$(read_public_key | tr -d '\r')"
	validate_public_key_line "${public_key}"

	home_dir="$(getent passwd "${ADMIN_USER}" | cut -d: -f6)"
	[[ -n "${home_dir}" ]] || die "could not determine home directory for ${ADMIN_USER}"
	auth_file="${home_dir}/.ssh/authorized_keys"
	install -d -o "${ADMIN_USER}" -g "${ADMIN_USER}" -m 700 "${home_dir}/.ssh"
	touch "${auth_file}"
	chown "${ADMIN_USER}:${ADMIN_USER}" "${auth_file}"
	chmod 600 "${auth_file}"
	if ! grep -qxF "${public_key}" "${auth_file}"; then
		printf '%s\n' "${public_key}" >>"${auth_file}"
	fi
	chown "${ADMIN_USER}:${ADMIN_USER}" "${auth_file}"
	chmod 600 "${auth_file}"
}

harden_ssh() {
	[[ "${HARDEN_SSH}" -eq 1 ]] || return

	log "Hardening sshd configuration"
	install -d -m 755 /etc/ssh/sshd_config.d
	cat >/etc/ssh/sshd_config.d/01-${APP_NAME}-hardening.conf <<'EOF'
PermitRootLogin no
PasswordAuthentication no
KbdInteractiveAuthentication no
PubkeyAuthentication yes
AuthenticationMethods publickey
EOF
	sshd -t
	if systemctl list-unit-files ssh.service >/dev/null 2>&1; then
		systemctl reload ssh || systemctl restart ssh
	elif systemctl list-unit-files sshd.service >/dev/null 2>&1; then
		systemctl reload sshd || systemctl restart sshd
	else
		warn "could not find ssh or sshd systemd service to reload"
	fi
}

configure_ufw() {
	[[ "${CONFIGURE_UFW}" -eq 1 ]] || return

	log "Configuring UFW"
	ufw default deny incoming
	ufw default allow outgoing
	ufw allow OpenSSH
	ufw allow 80/tcp
	ufw allow 443/tcp
	ufw --force enable
}

install_caddy() {
	[[ "${INSTALL_CADDY}" -eq 1 ]] || return

	log "Installing Caddy"
	export DEBIAN_FRONTEND=noninteractive
	apt-get install -y debian-keyring debian-archive-keyring apt-transport-https
	install -d -m 755 /usr/share/keyrings /etc/apt/sources.list.d
	curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' \
		| gpg --dearmor >/usr/share/keyrings/caddy-stable-archive-keyring.gpg
	curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' \
		>/etc/apt/sources.list.d/caddy-stable.list
	apt-get update
	apt-get install -y caddy
	systemctl enable --now caddy
}

clone_and_build() {
	log "Cloning ${REPO_URL} (${REPO_REF}) and building binaries"
	BUILD_DIR="$(mktemp -d)"
	git clone "${REPO_URL}" "${BUILD_DIR}"
	pushd "${BUILD_DIR}" >/dev/null
	git checkout "${REPO_REF}"
	mkdir -p bin
	/usr/local/go/bin/go build -o "bin/${APP_NAME}-web" ./cmd/web
	/usr/local/go/bin/go build -o "bin/${APP_NAME}-ingest" ./cmd/ingest
	/usr/local/go/bin/go build -o "bin/${APP_NAME}-admin-password-hash" ./cmd/admin-password-hash
	popd >/dev/null
}

ensure_admin_password_hash() {
	if [[ -n "${ADMIN_PASSWORD_HASH}" ]]; then
		return
	fi
	if [[ ! -t 0 ]]; then
		die "--admin-password-hash is required when stdin is not a terminal"
	fi

	local passphrase confirm
	read -r -s -p "Admin passphrase: " passphrase
	printf '\n' >&2
	read -r -s -p "Confirm admin passphrase: " confirm
	printf '\n' >&2
	[[ -n "${passphrase}" ]] || die "admin passphrase cannot be empty"
	[[ "${passphrase}" == "${confirm}" ]] || die "admin passphrases did not match"
	ADMIN_PASSWORD_HASH="$(printf '%s' "${passphrase}" | "${BUILD_DIR}/bin/${APP_NAME}-admin-password-hash")"
	unset passphrase confirm
}

install_app_files() {
	log "Installing app files to ${APP_DIR}"
	install -d -o root -g root -m 755 "${APP_DIR}" "${APP_DIR}/bin" "${APP_DIR}/config"
	install -o root -g root -m 755 "${BUILD_DIR}/bin/${APP_NAME}-web" "${APP_DIR}/bin/${APP_NAME}-web"
	install -o root -g root -m 755 "${BUILD_DIR}/bin/${APP_NAME}-ingest" "${APP_DIR}/bin/${APP_NAME}-ingest"

	rm -rf "${APP_DIR}/config"
	install -d -o root -g root -m 755 "${APP_DIR}/config"
	cp -R "${BUILD_DIR}/config/." "${APP_DIR}/config/"
	chown -R root:root "${APP_DIR}/config"
	find "${APP_DIR}/config" -type d -exec chmod 755 {} \;
	find "${APP_DIR}/config" -type f -exec chmod 644 {} \;
}

write_env_file() {
	log "Writing ${ENV_FILE}"
	local tmp
	tmp="$(mktemp)"
	cat >"${tmp}" <<EOF
ADDR=127.0.0.1:8080
DB_PATH=${DATA_DIR}/${APP_NAME}.db
MEDIA_ROOT=${DATA_DIR}/media
MEDIA_URL_PREFIX=/media
LOG_LEVEL=info
LOG_FORMAT=json
ADMIN_COOKIE_SECURE=true
ADMIN_PASSWORD_HASH=${ADMIN_PASSWORD_HASH}
EOF
	install -o root -g "${SERVICE_USER}" -m 640 "${tmp}" "${ENV_FILE}"
	rm -f "${tmp}"
}

write_systemd_units() {
	log "Writing systemd units"
	cat >/etc/systemd/system/${APP_NAME}-web.service <<EOF
[Unit]
Description=Sheaflive web app
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_USER}
WorkingDirectory=${APP_DIR}
EnvironmentFile=${ENV_FILE}
ExecStart=${APP_DIR}/bin/${APP_NAME}-web
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

	cat >/etc/systemd/system/${APP_NAME}-ingest.service <<EOF
[Unit]
Description=Sheaflive ingest
After=network-online.target ${APP_NAME}-web.service
Wants=network-online.target

[Service]
Type=oneshot
User=${SERVICE_USER}
Group=${SERVICE_USER}
WorkingDirectory=${APP_DIR}
EnvironmentFile=${ENV_FILE}
ExecStart=${APP_DIR}/bin/${APP_NAME}-ingest
EOF

	cat >/etc/systemd/system/${APP_NAME}-ingest.timer <<EOF
[Unit]
Description=Run Sheaflive ingest every 6 hours

[Timer]
OnBootSec=5min
OnUnitActiveSec=6h
Persistent=true
Unit=${APP_NAME}-ingest.service

[Install]
WantedBy=timers.target
EOF

	systemctl daemon-reload
	systemctl enable --now ${APP_NAME}-web.service
	systemctl enable --now ${APP_NAME}-ingest.timer
}

write_caddyfile() {
	[[ "${INSTALL_CADDY}" -eq 1 ]] || return

	log "Writing Caddyfile for ${DOMAIN}"
	cat >/etc/caddy/Caddyfile <<EOF
www.${DOMAIN} {
	redir https://${DOMAIN}{uri} permanent
}

${DOMAIN} {
	reverse_proxy 127.0.0.1:8080
}
EOF
	caddy validate --config /etc/caddy/Caddyfile
	systemctl reload caddy
}

set_timezone() {
	log "Setting timezone to ${TIMEZONE}"
	timedatectl set-timezone "${TIMEZONE}"
}

verify_local_app() {
	log "Verifying local app health"
	sleep 2
	curl -fsS http://127.0.0.1:8080/healthz >/dev/null
	curl -fsS http://127.0.0.1:8080/readyz >/dev/null
}

maybe_run_initial_ingest() {
	[[ "${RUN_INITIAL_INGEST}" -eq 1 ]] || return

	log "Running initial ingest"
	systemctl start ${APP_NAME}-ingest.service
	local result status
	result="$(systemctl show ${APP_NAME}-ingest.service -p Result --value)"
	status="$(systemctl show ${APP_NAME}-ingest.service -p ExecMainStatus --value)"
	[[ "${result}" == "success" && "${status}" == "0" ]] || die "initial ingest failed; inspect journalctl -u ${APP_NAME}-ingest"
}

warn_if_dns_not_ready() {
	[[ "${INSTALL_CADDY}" -eq 1 ]] || return

	local public_ipv4 domain_ipv4
	public_ipv4="$(curl -fsS https://api.ipify.org || true)"
	domain_ipv4="$(getent ahostsv4 "${DOMAIN}" | awk 'NR == 1 {print $1}' || true)"
	if [[ -n "${public_ipv4}" && -n "${domain_ipv4}" && "${public_ipv4}" != "${domain_ipv4}" ]]; then
		warn "${DOMAIN} currently resolves to ${domain_ipv4}, but this server appears to be ${public_ipv4}"
		warn "Caddy is configured, but certificate issuance may wait until DNS points at this VPS"
	fi
}

print_summary() {
	cat <<EOF

Bootstrap complete.

Check services:
  systemctl status ${APP_NAME}-web --no-pager
  systemctl list-timers ${APP_NAME}-ingest.timer --no-pager
  journalctl -u ${APP_NAME}-web -n 80 --no-pager
  journalctl -u ${APP_NAME}-ingest -n 80 --no-pager

Local health checks:
  curl -fsS http://127.0.0.1:8080/healthz
  curl -fsS http://127.0.0.1:8080/readyz

Public checks, after DNS and TLS are ready:
  curl -I https://${DOMAIN}/healthz
  curl -I https://www.${DOMAIN}/healthz

Persistent data:
  ${DATA_DIR}/${APP_NAME}.db
  ${DATA_DIR}/media
EOF
}

main() {
	parse_args "$@"
	require_root
	validate_inputs
	install_packages
	set_timezone
	install_go
	ensure_admin_user_and_key
	ensure_service_user_and_dirs
	harden_ssh
	configure_ufw
	install_caddy
	clone_and_build
	ensure_admin_password_hash
	install_app_files
	write_env_file
	write_systemd_units
	write_caddyfile
	verify_local_app
	maybe_run_initial_ingest
	warn_if_dns_not_ready
	print_summary
}

main "$@"
