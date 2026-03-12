#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMPLATE_PATH="${ROOT_DIR}/config.yaml.template"
TMP_DIR="${ROOT_DIR}/.tmp"

ROLE="${1:-active}"

ACTIVE_PORT="${ACTIVE_PORT:-6065}"
STANDBY_PORT="${STANDBY_PORT:-6066}"

DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-postgres}"
DB_NAME="${DB_NAME:-warehouse}"
DB_SSL_MODE="${DB_SSL_MODE:-disable}"

JWT_SECRET="${JWT_SECRET:-local-dev-jwt-secret-at-least-32-chars}"
INTERNAL_SHARED_SECRET="${INTERNAL_SHARED_SECRET:-local-dev-internal-shared-secret}"

info() { printf '%s\n' "$*"; }
err() { printf '%s\n' "$*" >&2; }

usage() {
  cat <<'EOF'
用法：
  bash scripts/local.sh
  bash scripts/local.sh active
  bash scripts/local.sh standby

默认行为：
  - 不传参数时启动 active
  - 配置文件从 config.yaml.template 生成到 .tmp/active.yaml 或 .tmp/standby.yaml
  - 数据目录分别使用 .tmp/active/data 和 .tmp/standby/data
  - 使用 go run ./cmd/warehouse -c <generated-config> 前台启动

可选环境变量：
  ACTIVE_PORT / STANDBY_PORT
  DB_HOST / DB_PORT / DB_USER / DB_PASSWORD / DB_NAME / DB_SSL_MODE
  JWT_SECRET
  INTERNAL_SHARED_SECRET
EOF
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    err "缺少依赖命令: $1"
    exit 1
  fi
}

render_config() {
  local role="$1"
  local output_path="$2"
  local node_id node_role port peer_node_id peer_base_url data_dir

  case "$role" in
    active)
      node_id="warehouse-active"
      node_role="active"
      port="${ACTIVE_PORT}"
      peer_node_id="warehouse-standby"
      peer_base_url="http://127.0.0.1:${STANDBY_PORT}"
      data_dir="${TMP_DIR}/active/data"
      ;;
    standby)
      node_id="warehouse-standby"
      node_role="standby"
      port="${STANDBY_PORT}"
      peer_node_id="warehouse-active"
      peer_base_url=""
      data_dir="${TMP_DIR}/standby/data"
      ;;
    *)
      err "不支持的角色: ${role}"
      usage
      exit 1
      ;;
  esac

  cp "${TEMPLATE_PATH}" "${output_path}"

  # 只覆盖本地双实例启动所需的关键字段，其他字段保持模板默认值。
  awk \
    -v port="${port}" \
    -v db_host="${DB_HOST}" \
    -v db_port="${DB_PORT}" \
    -v db_user="${DB_USER}" \
    -v db_password="${DB_PASSWORD}" \
    -v db_name="${DB_NAME}" \
    -v db_ssl_mode="${DB_SSL_MODE}" \
    -v node_id="${node_id}" \
    -v node_role="${node_role}" \
    -v peer_node_id="${peer_node_id}" \
    -v peer_base_url="${peer_base_url}" \
    -v shared_secret="${INTERNAL_SHARED_SECRET}" \
    -v data_dir="${data_dir}" \
    -v jwt_secret="${JWT_SECRET}" \
    '
    BEGIN {
      section = ""
      subsection = ""
    }
    /^server:$/ {
      section = "server"
      subsection = ""
      print
      next
    }
    /^database:$/ {
      section = "database"
      subsection = ""
      print
      next
    }
    /^node:$/ {
      section = "node"
      subsection = ""
      print
      next
    }
    /^internal:$/ {
      section = "internal"
      subsection = ""
      print
      next
    }
    /^  replication:$/ && section == "internal" {
      subsection = "replication"
      print
      next
    }
    /^webdav:$/ {
      section = "webdav"
      subsection = ""
      print
      next
    }
    /^web3:$/ {
      section = "web3"
      subsection = ""
      print
      next
    }
    /^  ucan:$/ && section == "web3" {
      subsection = "ucan"
      print
      next
    }
    /^email:$/ {
      section = "email"
      subsection = ""
      print
      next
    }
    /^security:$/ {
      section = "security"
      subsection = ""
      print
      next
    }
    /^cors:$/ {
      section = "cors"
      subsection = ""
      print
      next
    }
    /^log:$/ {
      section = "log"
      subsection = ""
      print
      next
    }
    section == "server" && $1 == "address:" {
      print "  address: \"127.0.0.1\""
      next
    }
    section == "server" && $1 == "port:" {
      print "  port: " port
      next
    }
    section == "database" && $1 == "host:" {
      print "  host: \"" db_host "\""
      next
    }
    section == "database" && $1 == "port:" {
      print "  port: " db_port
      next
    }
    section == "database" && $1 == "username:" {
      print "  username: \"" db_user "\""
      next
    }
    section == "database" && $1 == "password:" {
      print "  password: \"" db_password "\""
      next
    }
    section == "database" && $1 == "database:" {
      print "  database: \"" db_name "\""
      next
    }
    section == "database" && $1 == "ssl_mode:" {
      print "  ssl_mode: \"" db_ssl_mode "\""
      next
    }
    section == "node" && $1 == "id:" {
      print "  id: \"" node_id "\""
      next
    }
    section == "node" && $1 == "role:" {
      print "  role: \"" node_role "\""
      next
    }
    section == "internal" && subsection == "replication" && $1 == "enabled:" {
      print "    enabled: true"
      next
    }
    section == "internal" && subsection == "replication" && $1 == "peer_node_id:" {
      print "    peer_node_id: \"" peer_node_id "\""
      next
    }
    section == "internal" && subsection == "replication" && $1 == "peer_base_url:" {
      print "    peer_base_url: \"" peer_base_url "\""
      next
    }
    section == "internal" && subsection == "replication" && $1 == "shared_secret:" {
      print "    shared_secret: \"" shared_secret "\""
      next
    }
    section == "internal" && subsection == "replication" && $1 == "dispatch_interval:" {
      print "    dispatch_interval: 1s"
      next
    }
    section == "internal" && subsection == "replication" && $1 == "request_timeout:" {
      print "    request_timeout: 10s"
      next
    }
    section == "internal" && subsection == "replication" && $1 == "retry_backoff_base:" {
      print "    retry_backoff_base: 1s"
      next
    }
    section == "internal" && subsection == "replication" && $1 == "max_retry_backoff:" {
      print "    max_retry_backoff: 10s"
      next
    }
    section == "webdav" && $1 == "directory:" {
      print "  directory: \"" data_dir "\""
      next
    }
    section == "web3" && subsection == "" && $1 == "jwt_secret:" {
      print "  jwt_secret: \"" jwt_secret "\""
      next
    }
    section == "web3" && subsection == "ucan" && $1 == "enabled:" {
      print "    enabled: false"
      next
    }
    section == "email" && $1 == "enabled:" {
      print "  enabled: false"
      next
    }
    section == "security" && $1 == "behind_proxy:" {
      print "  behind_proxy: false"
      next
    }
    section == "cors" && $1 == "enabled:" {
      print "  enabled: false"
      next
    }
    section == "cors" && $1 == "credentials:" {
      print "  credentials: false"
      next
    }
    section == "log" && $1 == "level:" {
      print "  level: \"debug\""
      next
    }
    {
      print
    }
    ' "${output_path}" > "${output_path}.tmp"

  mv "${output_path}.tmp" "${output_path}"
}

main() {
  case "${ROLE}" in
    active|"")
      ROLE="active"
      ;;
    standby)
      ;;
    -h|--help|help)
      usage
      exit 0
      ;;
    *)
      err "不支持的参数: ${ROLE}"
      usage
      exit 1
      ;;
  esac

  require_command go
  require_command awk

  if [[ ! -f "${TEMPLATE_PATH}" ]]; then
    err "缺少模板文件: ${TEMPLATE_PATH}"
    exit 1
  fi

  mkdir -p "${TMP_DIR}/active/data" "${TMP_DIR}/standby/data"

  local config_path
  config_path="${TMP_DIR}/${ROLE}.yaml"
  render_config "${ROLE}" "${config_path}"

  info "role=${ROLE}"
  info "config=${config_path}"
  info "data_dir=${TMP_DIR}/${ROLE}/data"
  info "command=go run ./cmd/warehouse -c ${config_path}"

  cd "${ROOT_DIR}"
  exec go run ./cmd/warehouse -c "${config_path}"
}

main "$@"
