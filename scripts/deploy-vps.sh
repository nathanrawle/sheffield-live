#!/usr/bin/env bash
set -euo pipefail

APP_NAME="sheaflive"
REPO_URL=""
REPO_REF="main"
SOURCE_DIR=""
GO_BIN="/usr/local/go/bin/go"
SKIP_RESTART=0
SKIP_HEALTH_CHECK=0

APP_DIR="/opt/${APP_NAME}"
BUILD_DIR=""
CLONE_DIR=""

usage() {
	cat <<'EOF'
Usage:
  sudo bash scripts/deploy-vps.sh (--repo-url REPO_URL | --source-dir PATH) [options]

Source options:
  --repo-url REPO_URL        Git clone URL to build and deploy.
  --repo-ref REF             Branch, tag, or commit to deploy. Default: main.
  --source-dir PATH          Existing checkout to build and deploy.

Other options:
  --app-name NAME            Deployment name. Default: sheaflive.
  --go-bin PATH              Go binary path. Default: /usr/local/go/bin/go.
  --skip-restart             Install files but do not restart the web service.
  --skip-health-check        Do not check local health/readiness after restart.

Examples:
  sudo bash scripts/deploy-vps.sh \
    --repo-url https://github.com/OWNER/REPO.git \
    --repo-ref main

  sudo bash scripts/deploy-vps.sh --source-dir /home/nathan/src/sheaflive
EOF
}

log() {
	printf '[%s] %s\n' "$(date +'%Y-%m-%dT%H:%M:%S%z')" "$*" >&2
}

die() {
	printf 'error: %s\n' "$*" >&2
	exit 1
}

cleanup() {
	if [[ -n "${BUILD_DIR}" && -d "${BUILD_DIR}" ]]; then
		rm -rf "${BUILD_DIR}"
	fi
	if [[ -n "${CLONE_DIR}" && -d "${CLONE_DIR}" ]]; then
		rm -rf "${CLONE_DIR}"
	fi
}
trap cleanup EXIT

parse_args() {
	while [[ $# -gt 0 ]]; do
		case "$1" in
		--repo-url)
			REPO_URL="${2:-}"
			shift 2
			;;
		--repo-ref)
			REPO_REF="${2:-}"
			shift 2
			;;
		--source-dir)
			SOURCE_DIR="${2:-}"
			shift 2
			;;
		--app-name)
			APP_NAME="${2:-}"
			APP_DIR="/opt/${APP_NAME}"
			shift 2
			;;
		--go-bin)
			GO_BIN="${2:-}"
			shift 2
			;;
		--skip-restart)
			SKIP_RESTART=1
			shift
			;;
		--skip-health-check)
			SKIP_HEALTH_CHECK=1
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
	if [[ -n "${REPO_URL}" && -n "${SOURCE_DIR}" ]]; then
		die "use either --repo-url or --source-dir, not both"
	fi
	if [[ -z "${REPO_URL}" && -z "${SOURCE_DIR}" ]]; then
		die "--repo-url or --source-dir is required"
	fi
	[[ -x "${GO_BIN}" ]] || die "Go binary is not executable: ${GO_BIN}"
	command -v git >/dev/null 2>&1 || die "git is required"
	if [[ "${SKIP_RESTART}" -eq 0 ]]; then
		command -v systemctl >/dev/null 2>&1 || die "systemctl is required to restart the web service"
	fi
	if [[ "${SKIP_HEALTH_CHECK}" -eq 0 ]]; then
		command -v curl >/dev/null 2>&1 || die "curl is required for health checks"
	fi
}

prepare_source_dir() {
	if [[ -n "${SOURCE_DIR}" ]]; then
		[[ -d "${SOURCE_DIR}" ]] || die "source directory does not exist: ${SOURCE_DIR}"
		[[ -d "${SOURCE_DIR}/config" ]] || die "source directory has no config directory: ${SOURCE_DIR}"
		return
	fi

	CLONE_DIR="$(mktemp -d)"
	log "Cloning ${REPO_URL} (${REPO_REF})"
	git clone "${REPO_URL}" "${CLONE_DIR}"
	pushd "${CLONE_DIR}" >/dev/null
	git checkout "${REPO_REF}"
	popd >/dev/null
	SOURCE_DIR="${CLONE_DIR}"
}

build_binaries() {
	log "Building ${APP_NAME} binaries"
	BUILD_DIR="$(mktemp -d)"
	pushd "${SOURCE_DIR}" >/dev/null
	"${GO_BIN}" build -o "${BUILD_DIR}/${APP_NAME}-web" ./cmd/web
	"${GO_BIN}" build -o "${BUILD_DIR}/${APP_NAME}-ingest" ./cmd/ingest
	popd >/dev/null
}

install_app_files() {
	log "Installing app files to ${APP_DIR}"
	install -d -o root -g root -m 755 "${APP_DIR}" "${APP_DIR}/bin" "${APP_DIR}/config"
	install -o root -g root -m 755 "${BUILD_DIR}/${APP_NAME}-web" "${APP_DIR}/bin/${APP_NAME}-web"
	install -o root -g root -m 755 "${BUILD_DIR}/${APP_NAME}-ingest" "${APP_DIR}/bin/${APP_NAME}-ingest"

	rm -rf "${APP_DIR}/config"
	install -d -o root -g root -m 755 "${APP_DIR}/config"
	cp -R "${SOURCE_DIR}/config/." "${APP_DIR}/config/"
	chown -R root:root "${APP_DIR}/config"
	find "${APP_DIR}/config" -type d -exec chmod 755 {} \;
	find "${APP_DIR}/config" -type f -exec chmod 644 {} \;
}

restart_web_service() {
	[[ "${SKIP_RESTART}" -eq 0 ]] || return

	log "Restarting ${APP_NAME}-web.service"
	systemctl restart "${APP_NAME}-web.service"
}

verify_local_app() {
	[[ "${SKIP_HEALTH_CHECK}" -eq 0 ]] || return

	log "Verifying local app health"
	sleep 2
	curl -fsS http://127.0.0.1:8080/healthz >/dev/null
	curl -fsS http://127.0.0.1:8080/readyz >/dev/null
}

print_summary() {
	cat <<EOF

Deploy complete.

Check service:
  systemctl status ${APP_NAME}-web --no-pager
  journalctl -u ${APP_NAME}-web -n 80 --no-pager
EOF
}

main() {
	parse_args "$@"
	require_root
	validate_inputs
	prepare_source_dir
	build_binaries
	install_app_files
	restart_web_service
	verify_local_app
	print_summary
}

main "$@"
