#!/usr/bin/env bash
set -euo pipefail

info() { printf '%s\n' "$*"; }
err() { printf '%s\n' "$*" >&2; }

usage() {
  cat <<'EOF'
用法：
  bash scripts/bootstrap_standby.sh \
    --standby-base-url https://warehouse-standby.internal \
    --source-node-id warehouse-active \
    --shared-secret your-secret

可选参数：
  --outbox-id N
      显式指定 bootstrap baseline 的 outboxId。
      不传时，standby 会使用当前 source -> standby 的最大 outbox 序号。

  --status-only
      只查询 standby 当前 replication status，不执行 bootstrap/mark。

环境变量：
  STANDBY_BASE_URL
  SOURCE_NODE_ID
  SHARED_SECRET
  OUTBOX_ID
EOF
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    err "缺少依赖命令: $1"
    exit 1
  fi
}

sha256_hex() {
  local input="$1"
  printf '%s' "$input" | openssl dgst -sha256 -binary | xxd -p -c 256
}

hmac_hex() {
  local input="$1"
  local secret="$2"
  printf '%s' "$input" | openssl dgst -sha256 -hmac "$secret" -binary | xxd -p -c 256
}

sign_request() {
  local method="$1"
  local path="$2"
  local node_id="$3"
  local timestamp="$4"
  local payload_hash="$5"
  local secret="$6"
  local normalized_hash payload
  normalized_hash="${payload_hash:-UNSIGNED-PAYLOAD}"
  payload="$(printf '%s\n%s\n%s\n%s\n%s' \
    "$(printf '%s' "$method" | tr '[:lower:]' '[:upper:]')" \
    "$path" \
    "$node_id" \
    "$timestamp" \
    "$normalized_hash")"
  hmac_hex "$payload" "$secret"
}

pretty_print_json() {
  local body="$1"
  if command -v jq >/dev/null 2>&1; then
    printf '%s' "$body" | jq .
  else
    printf '%s\n' "$body"
  fi
}

request_internal() {
  local method="$1"
  local full_url="$2"
  local path="$3"
  local payload="$4"
  local payload_hash="$5"
  local node_id="$6"
  local secret="$7"

  local timestamp signature response_file status_code normalized_hash
  timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  normalized_hash="${payload_hash:-UNSIGNED-PAYLOAD}"
  signature="$(sign_request "$method" "$path" "$node_id" "$timestamp" "$normalized_hash" "$secret")"
  response_file="$(mktemp)"
  trap 'rm -f "$response_file"' RETURN

  local -a curl_args=(
    -sS
    -X "$method"
    -H "X-Warehouse-Node-Id: $node_id"
    -H "X-Warehouse-Timestamp: $timestamp"
    -H "X-Warehouse-Signature: $signature"
    -H "X-Warehouse-Content-SHA256: $normalized_hash"
    -o "$response_file"
    -w '%{http_code}'
  )
  if [[ -n "$payload" ]]; then
    curl_args+=(
      -H "Content-Type: application/json"
      --data-binary "$payload"
    )
  fi
  curl_args+=("$full_url")

  status_code="$(curl "${curl_args[@]}")"

  RESPONSE_STATUS_CODE="$status_code"
  RESPONSE_BODY="$(cat "$response_file")"
  rm -f "$response_file"
  trap - RETURN
}

STANDBY_BASE_URL="${STANDBY_BASE_URL:-}"
SOURCE_NODE_ID="${SOURCE_NODE_ID:-}"
SHARED_SECRET="${SHARED_SECRET:-}"
OUTBOX_ID="${OUTBOX_ID:-}"
STATUS_ONLY="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --standby-base-url)
      STANDBY_BASE_URL="${2:-}"
      shift 2
      ;;
    --source-node-id)
      SOURCE_NODE_ID="${2:-}"
      shift 2
      ;;
    --shared-secret)
      SHARED_SECRET="${2:-}"
      shift 2
      ;;
    --outbox-id)
      OUTBOX_ID="${2:-}"
      shift 2
      ;;
    --status-only)
      STATUS_ONLY="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      err "未知参数: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$STANDBY_BASE_URL" || -z "$SOURCE_NODE_ID" || -z "$SHARED_SECRET" ]]; then
  err "缺少必要参数。"
  usage
  exit 1
fi

require_command curl
require_command openssl
require_command xxd

STANDBY_BASE_URL="${STANDBY_BASE_URL%/}"
STATUS_PATH="/api/v1/internal/replication/status"
BOOTSTRAP_PATH="/api/v1/internal/replication/bootstrap/mark"

if [[ "$STATUS_ONLY" != "true" ]]; then
  payload='{}'
  if [[ -n "$OUTBOX_ID" ]]; then
    payload="{\"outboxId\":${OUTBOX_ID}}"
  fi
  payload_hash="$(sha256_hex "$payload")"

  info "调用 standby bootstrap/mark ..."
  request_internal "POST" "${STANDBY_BASE_URL}${BOOTSTRAP_PATH}" "$BOOTSTRAP_PATH" "$payload" "$payload_hash" "$SOURCE_NODE_ID" "$SHARED_SECRET"

  if [[ "$RESPONSE_STATUS_CODE" != "200" ]]; then
    err "bootstrap/mark 失败，HTTP ${RESPONSE_STATUS_CODE}"
    pretty_print_json "$RESPONSE_BODY"
    exit 1
  fi

  info "bootstrap/mark 成功："
  pretty_print_json "$RESPONSE_BODY"
  printf '\n'
fi

info "查询 standby replication status ..."
request_internal "GET" "${STANDBY_BASE_URL}${STATUS_PATH}" "$STATUS_PATH" "" "" "$SOURCE_NODE_ID" "$SHARED_SECRET"

if [[ "$RESPONSE_STATUS_CODE" != "200" ]]; then
  err "status 查询失败，HTTP ${RESPONSE_STATUS_CODE}"
  pretty_print_json "$RESPONSE_BODY"
  exit 1
fi

pretty_print_json "$RESPONSE_BODY"
