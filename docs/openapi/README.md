# Warehouse OpenAPI

`warehouse.openapi.yaml` 是 Warehouse JSON HTTP API 的正式接口契约，使用 OpenAPI 3.1。

## 文档边界

- OpenAPI 描述 `/api/v1/public/*` 和后续纳入的 `/api/v1/admin/*` JSON API。
- WebDAV `/dav/*` 的协议方法、请求头和客户端配置继续由 Markdown 使用指南维护。
- 独立 S3 Endpoint 的 Signature V4、bucket/key 和兼容操作由 [S3 设计方案](../S3设计方案.md) 维护。
- 不对外发布 `/api/v1/internal/*` 内部复制 API。

## 当前覆盖范围

第一批已建模：

- 健康检查
- 钱包挑战/签名、密码登录、令牌刷新和退出
- 资产空间
- 资产对象 metadata、download、write 和 list
- 当前用户和配额
- WebDAV 访问密钥
- S3 凭证
- 用户通知、通知偏好和未读数 SSE
- 分组和成员管理，包括邀请确认/拒绝
- 回收站
- 管理员公告、通知已读和未读数 SSE
- Email 验证码登录
- 管理员用户管理
- 公开分享的创建、列表、撤销和文件访问
- 定向分享的创建、列表、撤销和受众查询
- 定向分享目录的列表、下载、上传、建目录、重命名和删除
- 可恢复分片上传会话

当前 Router 中的对外 JSON、SSE、文件流、资产对象和分片上传 API 已全部建模。`/api/v1/public/share/` 和 `/api/v1/public/uploads/sessions/` 在 Go Router 中是动态前缀，OpenAPI 已使用具体的 token、filename、sessionId 和 partNumber 路径表达。

`/api/v1/public/webdav/address/*` 历史别名已从 Router 移除，调用方必须使用 OpenAPI 中的 `/api/v1/public/webdav/group/*` 路径。

`/api/v1/internal/*` 内部复制接口按设计不进入对外规范；WebDAV 协议操作由用户使用指南维护，独立 S3 Endpoint 的协议操作由 [S3 设计方案](../S3设计方案.md) 维护。新增或修改对外 HTTP API 时，必须同步修改 OpenAPI。

## 校验和预览

使用 Redocly CLI 校验：

```bash
cd web
npm run openapi:lint
```

脚本按精确版本临时执行 Redocly CLI 和 openapi-typescript，避免将完整文档工具链常驻加入前端依赖。仓库根目录的 `redocly.yaml` 启用推荐规则，只关闭与当前项目事实不匹配的 license、本地 server 和强制 4xx 响应提示。

生成单文件规范：

```bash
npx --yes @redocly/cli bundle docs/openapi/warehouse.openapi.yaml \
  --output docs/openapi/warehouse.openapi.bundle.yaml
```

本地预览：

```bash
npx --yes @redocly/cli preview-docs docs/openapi/warehouse.openapi.yaml
```

生成前端 TypeScript 类型：

```bash
cd web
npm run openapi:generate
```

提交前同时校验规范和生成文件是否漂移：

```bash
cd web
npm run openapi:check
```

CI 会执行同一条 `openapi:check`。如果 OpenAPI 已修改但 `src/api/generated/openapi.d.ts` 没有重新生成，检查会失败。

这些命令不要将生成的 bundle 当作源文件编辑；源文件始终是 `warehouse.openapi.yaml`。

## 维护规则

1. `operationId` 在整份规范中必须唯一且保持稳定。
2. 请求和响应应优先引用 `components/schemas` 中的命名模型。
3. 必须按当前 Handler 实际返回值记录状态码和 Content-Type，不得把纯文本错误虚构成统一 JSON。
4. Secret、Token 等敏感响应字段必须标记 `writeOnly: true`，示例不得放入真实凭证。
5. WebDAV 和 S3 的协议行为不通过自定义 OpenAPI 路径伪装成普通 JSON API。
