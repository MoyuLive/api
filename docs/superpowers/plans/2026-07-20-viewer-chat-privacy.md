# Yantube Viewer, Chat, and Privacy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在不修改 `deploy/` 的前提下，为 Yantube 交付可注册/登录的观众账号、按平台身份去重的实时观看人数、登录用户与游客实时弹幕，以及登录要求和房间密码可独立启用的隐私直播。

**Architecture:** Rust API 复用现有 `user`/JWT/PBKDF2，新增 15 分钟、房间绑定且带 `access_revision` 的 room ticket；所有访问入口在签发和 SRS/WebSocket 接纳时复用同一访问策略。单实例 `LiveHub` 用 Tokio 锁、`client_id -> viewer_key` 会话索引、身份引用计数和 `tokio::sync::broadcast` 同时承载唯一观众数与非持久化弹幕；React 房间页拥有 ticket、WebSocket 和访问门状态，播放器只接收 room ticket，WHEP/HLS/FLV 均把 ticket 放入查询参数。

**Tech Stack:** Rust 2021、axum 0.7（启用 `ws`）、SeaORM 0.12、Tokio、jsonwebtoken、PBKDF2-SHA256、PostgreSQL 18、React 18、TypeScript 5.9、Vite 5、MUI 5、Jotai、SRS 6、nginx、pnpm、PowerShell 7。

---

## 权威契约与非目标

- 权威设计：`api_rs/docs/superpowers/specs/2026-07-20-viewer-chat-privacy-design.md`。
- 只修改 `api_rs/` 与 `front/`；`deploy/` 仅用于原样启动和验证。
- 保留首账号 bootstrap：第一位公开注册用户仍为 `super_admin` 且创建兼容默认房间；之后公开注册为启用的 `user`，房间数为 0；管理员创建用户的既有默认房间行为不变。
- 不增加 Redis、消息队列、观众会话表、弹幕历史表、OAuth、找回密码、关注、礼物或审核后台。
- 唯一观众数只来自成功通过 `on_play` 的 SRS 播放会话；WebSocket 连接数和 SRS `clients` 字段都不得进入计数。
- WHEP Bearer 不作为鉴权依据；WHEP、HLS、HTTP-FLV 查询参数中的 room ticket 是唯一播放凭证。
- `deploy/srs/srs.conf` 当前启用 HLS 与 RTC/WHEP，但没有启用 `http_remux`；SRS 6 默认 `hls_ctx on`，真实 HLS 必须跟随 master/media playlist 返回的 `hls_ctx` URI。HTTP-FLV builder/ticket 契约仍完整实现并由单元测试覆盖，Compose 真实媒体只验收 HLS 与 WHEP；不得通过修改 `deploy/` 绕过。
- room ticket、账号 JWT、推流码、房间明文密码和密码哈希不得出现在 API/nginx 应用日志。
- 所有计划中的提交边界都受 Commit Guard 阻止；只有用户后续明确授权后才能执行对应 git 写操作。

## API、JSON 与 WebSocket 固定契约

### HTTP

```text
POST /api/account/create
request:  {"username":"viewer1","password":"viewer123"}
200 data: {"token":"signed-account-jwt"}

GET /api/live/rooms/:stream_id
200 data: {
  "stream_id":"room-1",
  "title":"晚间直播",
  "cover_url":"/uploads/covers/1.jpg",
  "status":"live",
  "require_login":true,
  "has_password":true,
  "viewer_count":2
}

POST /api/live/rooms/:stream_id/access
Authorization: Bearer signed-account-jwt  // 可省略
request:  {"guest_id":"550e8400-e29b-41d4-a716-446655440000","password":"room-pass"}
200 data: {
  "ticket":"signed-room-ticket",
  "expires_at":"2026-07-20T12:15:00Z",
  "viewer":{"kind":"user","name":"viewer1"}
}

PUT /api/live/rooms/:id/privacy
Authorization: Bearer signed-account-jwt
request:  {"require_login":true,"password_enabled":true,"password":"room-pass"}
200 data: {"require_login":true,"has_password":true}
```

`POST /access` 对游客返回 `viewer.kind = "guest"` 和确定性名称 `游客-XXXX`；已登录 ticket 忽略 `guest_id` 并返回 `viewer.kind = "user"`。HTTP 错误固定为：账号缺失/无效/停用 `401`，密码不匹配、策略过期或房间不可用 `403`，未知房间 `404`，UUID/密码长度/请求结构错误 `400`。

管理员房间请求在现有字段上增加：

```json
{
  "require_login": false,
  "password_enabled": true,
  "password": "room-pass"
}
```

`POST /api/admin/rooms` 的两个布尔值缺省为 `false`；`PUT /api/admin/rooms/:id` 的三个隐私字段均可省略。管理员和房主响应只返回 `require_login`、`has_password`，不返回 `password_hash` 或旧密码。

### WebSocket

```text
GET /api/live/rooms/:stream_id/ws?ticket=percent-encoded-signed-room-ticket
```

```json
{"type":"send_message","content":"hello"}
{"type":"viewer_count","count":2}
{"type":"danmaku","id":"nV8yJr2K4sQp6MxD","sender":{"kind":"guest","name":"游客-550E"},"content":"hello","sent_at":"2026-07-20T12:00:00Z"}
{"type":"error","code":"rate_limited","message":"发送太快"}
{"type":"error","code":"invalid_message","message":"弹幕必须为 1-100 个字符"}
```

- 无效 ticket 升级后立即以 WebSocket close code `1008`、固定 reason `room access denied` 关闭，且不读取客户端消息。
- 接纳后的连接即使 ticket 到期也可继续；重连必须重新取票。
- 消息先 `trim()`，再按 Unicode scalar value 计数 1-100；只把通过校验且未触发 1 秒限流的消息记为“accepted”。
- 服务端生成 ID、RFC3339 UTC 时间和发送者；前端输入不得覆盖发送者字段。

## 文件职责地图

### `api_rs/` 新建

| 文件 | 职责 |
|---|---|
| `migrations/09_add_live_room_privacy.sql` | 幂等增加 `require_login`、`password_hash`、`access_revision`，保证旧房间默认公开。 |
| `src/room_access.rs` | room ticket claims、签发/验签、UUID/游客名、四象限访问策略、隐私更新计算与密码校验；不处理 HTTP/WebSocket I/O。 |
| `src/room_privacy.rs` | 共用的 SeaORM 行锁事务更新 helper；owner/admin 的隐私变化必须在锁定行上重新计算并提交。 |
| `src/live_hub.rs` | 内存房间状态、SRS client 幂等索引、viewer 引用计数、当前人数及 broadcast 事件。 |
| `src/danmaku.rs` | 客户消息反序列化、纯文本长度校验、每连接限流、服务端弹幕构造。 |
| `src/handlers/room.rs` | 单房间公共元数据、取票、房主隐私更新、WebSocket 升级和 socket 循环。 |

### `api_rs/` 修改

| 文件 | 精确边界 |
|---|---|
| `Cargo.toml`、`Cargo.lock` | 为 axum 启用 `ws` feature；不引入 Redis/持久化聊天依赖。 |
| `src/db.rs` | `MIGRATIONS` 注册第 09 项；迁移测试执行两次并检查列默认值。 |
| `src/entities/live_room.rs` | `Model` 增加 3 个字段，`password_hash` 加 `#[serde(skip_serializing)]` 防御性脱敏。 |
| `src/auth.rs` | 提取可复用的账号 JWT 解码函数，保持现有 `CurrentUser` 行为。 |
| `src/main.rs` | 注册 `room_access`/`room_privacy`/hub/danmaku 模块、`AppState.live_hub`、四条新路由、仅记录 axum `MatchedPath` 模板的 HTTP tracing。 |
| `src/handlers/mod.rs` | 导出 `room` handler 模块。 |
| `src/handlers/account.rs` | 注册返回 JWT；只有首账号创建默认房间。 |
| `src/handlers/live.rs` | 公共列表加入隐私标志和 hub 人数；我的房间 DTO 加隐私标志；更新测试 fixture。 |
| `src/handlers/admin.rs` | 管理员房间 DTO/请求/创建/更新接入隐私字段；隐私更新调用共用锁定 helper，普通 admin 可改隐私但不能获得超管字段权限。 |
| `src/handlers/srs_callback.rs` | `on_play` ticket 接纳与计数、`on_stop` 幂等移除、`on_unpublish` 清零；删除推流码日志。 |
| `src/handlers/live_feed.rs` | 仅补齐 `live_room::Model` 测试 fixture；RSS 查询和输出行为保持不变。 |
| `src/handlers/playback.rs` | 测试 `AppState` 补 `LiveHub`；协议接口行为不变。 |

### `front/` 新建

| 文件 | 职责 |
|---|---|
| `src/libs/viewerIdentity.ts` | guest UUID 本地持久化、同源 redirect 清洗、Unicode/UTF-8 表单辅助校验。 |
| `src/libs/viewerIdentity.test.ts` | guest ID 稳定性、损坏值替换、redirect 开放跳转防护测试。 |
| `src/components/AccountActions.tsx` | 首页/房间共用的登录注册、用户名、管理入口和退出动作。 |
| `src/hooks/useRoomChannel.ts` | 房间元数据、访问门、取票、fresh-metadata 单周期恢复、ticket 到期判断、WebSocket 状态、封顶退避重连和事件状态。 |
| `src/libs/roomAccessState.ts` | stale metadata 恢复周期、最新 metadata gate 决策和“一次 refresh + 一次 access”预算的纯状态函数。 |
| `src/libs/roomAccessState.test.ts` | public→login、public→password、password change 与恢复预算防循环测试。 |
| `src/components/room/RoomAccessGate.tsx` | 加载、登录要求、密码要求、错误和重试界面；密码仅由父组件内存持有。 |
| `src/components/room/DanmakuPanel.tsx` | 连接状态、身份、字符余量、发送校验、可访问的最近消息区域。 |
| `src/components/room/PrivacyControls.tsx` | 房主和管理员共用的双开关、write-only 密码和保存状态表单。 |
| `src/components/player/DanmakuOverlay.tsx` | clipped、pointer-transparent 右向左弹幕轨道及 reduced-motion 六秒静态副本。 |
| `src/components/player/DanmakuOverlay.module.scss` | 仅使用 transform/opacity 的弹幕动画、轨道、裁剪和 reduced-motion 样式。 |

### `front/` 修改

| 文件 | 精确边界 |
|---|---|
| `src/libs/api.ts` | 注册/退出、公共元数据、取票、房主隐私 API、WS 数据类型、现有房间 DTO 隐私字段。 |
| `src/libs/streamUrls.ts`、`src/libs/streamUrls.test.ts` | WHEP/HLS/FLV 和 WebSocket URL 的编码 ticket 参数；推流 URL 契约不变。 |
| `src/pages/Login.tsx` | 登录/注册双模式、与 API 一致的校验、安全 redirect、注册后直接登录。 |
| `src/pages/Home.tsx` | 共用账号动作、人数、登录/密码 chips；轮询继续每 10 秒读取 hub 快照。 |
| `src/pages/Room.tsx` | 访问状态机、stale metadata 恢复、WebSocket、人数、弹幕、响应式 player/chat 布局。 |
| `src/pages/AdminStreamCode.tsx` | 选中房间隐私卡、ticket 化预览、密码房间预览门；无房间账号仍正常显示空态。 |
| `src/pages/AdminRooms.tsx` | 创建/编辑对话框加入双开关和 write-only 密码。 |
| `src/components/ProtectedRoute.tsx` | 未登录跳转时保留安全的当前路径到 `redirect`。 |
| `src/components/player/MoyuPlayer.tsx` | 必填 `ticket` 和弹幕 props，移除 localStorage JWT 读取并挂载 overlay。 |
| `src/components/player/playerSources.ts` | 三种播放源统一使用 room ticket。 |
| `src/components/player/playbackAdapters.ts` | WHEP 调用不再发送 Bearer；仅使用 URL ticket。 |
| `src/components/player/MoyuPlayer.module.scss` | 保证 overlay 位于视频上、控制栏下且不拦截指针。 |
| `front/nginx.conf` | `/api/` WebSocket Upgrade、无 query 的 access log format；保留限流、上传和媒体代理。 |

### 明确不修改

- `deploy/docker-compose.test.yml`、`deploy/srs/srs.conf` 及 `deploy/` 下所有文件。
- `api_rs/docs/superpowers/specs/2026-07-20-viewer-chat-privacy-design.md`。
- `front/DESIGN.md`。

## 任务依赖与可并行波次

| 波次 | 可执行任务 | 前置依赖 | 合并/复核边界 |
|---|---|---|---|
| 0 | Task 1 | 无 | 数据字段和旧 fixture 全部编译后再开放并行。 |
| 1 | Task 2、Task 3、Task 4、Task 9 | Task 1 | 四个任务分别限定访问域、hub、注册、前端纯契约，允许并行。 |
| 2 | Task 5 | Task 2、Task 3 | 公共 HTTP 契约与 `AppState` 完成。 |
| 3 | Task 6、Task 7、Task 8、Task 10、Task 11 | Task 6 依赖 Task 2/5；Task 7/8 依赖 Task 5；Task 10 依赖 Task 4/9；Task 11 依赖 Task 9 | 后端三个任务可并行；账号 UI 与播放器可并行。 |
| 4 | Task 12 | Task 5、Task 8、Task 9、Task 10、Task 11 | 房间页端到端接入，并锁定 stale metadata 恢复契约。 |
| 5 | Task 13、Task 14 | Task 6、Task 9、Task 10、Task 12 | 首页展示与隐私管理 UI 可并行，但二者都不得改共享 API 类型名称。 |
| 6 | Task 15 | Task 1-14 | 两仓库门禁、真实 PostgreSQL 并发、Compose API/WS/HLS/WHEP/响应式验证和 finally 清理。 |

并行任务完成后先运行各自局部门禁，再进入下一波；`api_rs/` 和 `front/` 是独立仓库，任何获授权的未来提交也必须分别提交。

---

### Task 1: 隐私迁移、SeaORM 实体与旧 fixture

**Files:**
- Create: `api_rs/migrations/09_add_live_room_privacy.sql`
- Modify: `api_rs/src/db.rs:5-14,71-88`
- Modify: `api_rs/src/entities/live_room.rs:4-18`
- Modify test fixtures: `api_rs/src/handlers/live.rs:1031-1044`
- Modify test fixtures: `api_rs/src/handlers/live_feed.rs:285-298`
- Modify test fixtures: `api_rs/src/handlers/srs_callback.rs:687-700`

- [ ] **Step 1: 写迁移注册 RED 测试**

在 `src/db.rs` 测试模块增加精确断言：`MIGRATIONS.len() == 9`，最后一项包含 `require_login`、`password_hash`、`access_revision`。运行前应因当前长度为 8 失败。

- [ ] **Step 2: 运行 RED 测试**

Run: `cargo test --locked db::tests::privacy_migration_is_registered_last`

Expected: FAIL，断言左值为 `8`、右值为 `9`。

- [ ] **Step 3: 新增幂等 SQL**

`migrations/09_add_live_room_privacy.sql` 必须完整为：

```sql
ALTER TABLE IF EXISTS live_room
ADD COLUMN IF NOT EXISTS require_login BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE IF EXISTS live_room
ADD COLUMN IF NOT EXISTS password_hash TEXT NOT NULL DEFAULT '';

ALTER TABLE IF EXISTS live_room
ADD COLUMN IF NOT EXISTS access_revision INTEGER NOT NULL DEFAULT 0;
```

- [ ] **Step 4: 注册迁移并扩展实体**

把 `include_str!("../migrations/09_add_live_room_privacy.sql")` 追加到 `MIGRATIONS`。在 `live_room::Model` 的 `enabled` 后加入：

```rust
pub require_login: bool,
#[serde(skip_serializing)]
pub password_hash: String,
pub access_revision: i32,
```

- [ ] **Step 5: 补齐所有现有 `live_room::Model` fixture**

三个 fixture 构造器都显式填入：

```rust
require_login: false,
password_hash: String::new(),
access_revision: 0,
```

不得借助 `Default` 隐藏字段遗漏；这样后续隐私测试可在构造器上覆盖字段。

- [ ] **Step 6: 把 PostgreSQL 迁移测试改为双执行**

`bundled_migrations_execute_against_postgres` 在同一连接上连续调用两次 `run_migrations(&db).await`，再查询 `information_schema.columns`，断言三个列的 `is_nullable = 'NO'`，默认值分别包含 `false`、空字符串和 `0`。环境变量未设置时仍保留现有显式 skip 输出。

- [ ] **Step 7: 运行 GREEN 与相邻回归**

Run: `cargo test --locked db::tests::privacy_migration_is_registered_last`

Expected: PASS。

Run: `cargo test --locked handlers::live::tests`

Expected: 现有公共房间、排序、标题、封面测试全部 PASS。

Run: `cargo test --locked handlers::live_feed::tests`

Expected: RSS 测试全部 PASS，证明隐私字段没有改变 feed 行为。

- [ ] **Step 8: 记录提交边界（BLOCKED pending explicit user permission）**

建议 semantic commit：`feat(api): add live room privacy columns`。不要执行 `git add` 或 `git commit`。

---

### Task 2: 集中 room-access 策略、ticket 与隐私计算

**Files:**
- Create/Test: `api_rs/src/room_access.rs`
- Modify: `api_rs/src/auth.rs:20-139`
- Modify: `api_rs/src/main.rs:1-7`

- [ ] **Step 1: 写访问矩阵与 ticket RED 测试**

测试模块覆盖 4 个 `(require_login, has_password)` 组合的允许/拒绝结果，并分别断言：错误签名、过期、错误 `kind`、跨房间、旧 `access_revision`、缺少账号 attestation、缺少密码 attestation 全部拒绝。用固定 UTC 时间注入签发/验证函数，禁止测试等待 15 分钟。

- [ ] **Step 2: 写身份与隐私 RED 测试**

增加这些精确案例：

```text
normalize_guest_id("550E8400-E29B-41D4-A716-446655440000")
  => "550e8400-e29b-41d4-a716-446655440000"
guest_display_name("550e8400-e29b-41d4-a716-446655440000") => "游客-550E"
password scalar count 5 => invalid
password scalar count 6 => valid
password scalar count 64 => valid
password scalar count 65 => invalid
already-enabled + empty password => preserve hash/revision
disabled -> enabled + empty password => reject
enabled -> disabled => clear hash and increment revision once
same plaintext password => preserve hash/revision
different plaintext password => replace hash and increment revision once
```

- [ ] **Step 3: 运行 RED 测试**

Run: `cargo test --locked room_access::tests`

Expected: FAIL，模块 `room_access` 尚未声明。

- [ ] **Step 4: 定义固定领域类型**

`src/room_access.rs` 定义并由后续任务复用：

```rust
pub const ROOM_TICKET_KIND: &str = "room_access";
pub const ROOM_TICKET_TTL_SECONDS: i64 = 15 * 60;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ViewerKind { User, Guest }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ViewerIdentity {
    pub kind: ViewerKind,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomTicketClaims {
    pub kind: String,
    pub stream_id: String,
    pub viewer_key: String,
    pub display_name: String,
    pub user_id: Option<i32>,
    pub account_verified: bool,
    pub password_verified: bool,
    pub access_revision: i32,
    pub iat: i64,
    pub exp: i64,
}

pub struct IssuedRoomTicket {
    pub token: String,
    pub expires_at: DateTime<Utc>,
    pub viewer: ViewerIdentity,
}

pub struct RoomPrivacyInput {
    pub require_login: bool,
    pub password_enabled: bool,
    pub password: Option<String>,
}

pub struct PrivacyMutation {
    pub require_login: bool,
    pub password_hash: String,
    pub access_revision: i32,
    pub changed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoomAccessError {
    MalformedGuestId,
    MalformedPassword,
    AccountRequired,
    PasswordDenied,
    InvalidTicket,
    ExpiredTicket,
    WrongRoom,
    StalePolicy,
    Internal,
}
```

HTTP handler 映射固定为：`MalformedGuestId/MalformedPassword -> 400`、`AccountRequired -> 401`、`PasswordDenied/StalePolicy -> 403`、`Internal -> 500`；callback/WS admission 对其余 ticket 错误只输出统一拒绝，不暴露分类。

- [ ] **Step 5: 实现可注入时间的 ticket 函数**

实现这些精确签名：

```rust
pub fn issue_room_ticket(
    room: &live_room::Model,
    viewer_key: String,
    viewer: ViewerIdentity,
    user_id: Option<i32>,
    account_verified: bool,
    password_verified: bool,
    secret: &str,
    now: DateTime<Utc>,
) -> Result<IssuedRoomTicket, RoomAccessError>;

pub fn admit_room_ticket(
    token: &str,
    expected_stream_id: &str,
    room: &live_room::Model,
    secret: &str,
    now: DateTime<Utc>,
) -> Result<RoomTicketClaims, RoomAccessError>;
```

`jsonwebtoken::Validation` 固定 HS256、验证签名但关闭库内系统时钟 expiry，随后用注入的 `now` 手工检查 `exp > now.timestamp()`；还必须检查显式 `kind`、stream、revision、viewer key/user ID 一致性和策略 attestations。

- [ ] **Step 6: 实现四象限策略**

实现：

```rust
pub fn evaluate_room_policy(
    require_login: bool,
    has_password: bool,
    account_verified: bool,
    password_verified: bool,
) -> Result<(), RoomAccessError>;
```

先检查登录要求，再检查密码要求；这样双开关房间对游客即使提交正确密码仍返回账号类错误。`admit_room_ticket` 必须调用此函数，HTTP 签发路径也必须调用同一函数。

- [ ] **Step 7: 实现 guest UUID 和展示名**

`normalize_guest_id(&str) -> Result<String, RoomAccessError>` 只接受 `8-4-4-4-12` 的 canonical UUID 形状和 ASCII 十六进制字符，输出小写。`guest_display_name(&str)` 移除连字符后取前四个十六进制字符并转大写，输出 `游客-XXXX`；不得接受客户端昵称。

- [ ] **Step 8: 实现隐私更新计算**

`prepare_privacy_update(room, input) -> Result<PrivacyMutation, RoomAccessError>` 按以下顺序执行：按 `chars().count()` 校验非空新密码 6-64；启用但当前无 hash 且输入为空则拒绝；已启用且输入为空则保留；输入密码与现有 hash 验证相同则不改 hash；关闭则清空 hash；任何开关或实际密码值变化只把 revision 增加 1。revision 溢出返回内部错误，不回绕。

- [ ] **Step 9: 提取账号 JWT 解码函数**

在 `auth.rs` 增加 `pub fn decode_jwt(token: &str, secret: &str) -> Result<Claims, jsonwebtoken::errors::Error>`，让现有 `CurrentUser::from_request_parts` 调用它；`create_jwt`、1 小时账号 token、middleware 的数据库 enabled 检查保持不变。

- [ ] **Step 10: 注册模块并运行 GREEN**

在 `main.rs` 增加 `mod room_access;`。

Run: `cargo test --locked room_access::tests`

Expected: 全部 PASS，包括四象限、15 分钟边界、签名、room/revision 绑定、guest 名和隐私 revision。

Run: `cargo test --locked auth`

Expected: PASS；若该过滤器当前没有独立测试，cargo 输出 0 tests 且退出码 0。

- [ ] **Step 11: 记录提交边界（BLOCKED pending explicit user permission）**

建议 semantic commit：`feat(api): add room access policy and tickets`。不要执行 git 写命令。

---

### Task 3: LiveHub 唯一观众计数与广播

**Files:**
- Create/Test: `api_rs/src/live_hub.rs`
- Modify: `api_rs/src/main.rs:1-7,30-34,238-248`
- Modify test state: `api_rs/src/handlers/playback.rs:52-93`
- Modify test state: `api_rs/src/handlers/srs_callback.rs:634-670`

- [ ] **Step 1: 写 LiveHub RED 测试**

测试必须逐个断言：初始 0；同一个 `client_id` 重复 play 幂等；同 viewer 两个 client 只计 1；第二 viewer 计 2；停止第一个 client 仍为 2；停止该 viewer 最后 client 后为 1；重复 stop/未知 stop 不降；同 client 改 viewer 正确转移引用；`clear_stream` 清到 0；WebSocket subscribe 本身不改变人数。

- [ ] **Step 2: 运行 RED 测试**

Run: `cargo test --locked live_hub::tests`

Expected: FAIL，模块尚未声明。

- [ ] **Step 3: 定义 hub 事件和内部状态**

使用以下边界：

```rust
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RoomEvent {
    ViewerCount { count: usize },
    Danmaku {
        id: String,
        sender: ViewerIdentity,
        content: String,
        sent_at: String,
    },
}

pub struct LiveHub {
    rooms: RwLock<HashMap<String, RoomState>>,
}

struct RoomState {
    clients: HashMap<String, String>,
    viewer_sessions: HashMap<String, usize>,
    events: broadcast::Sender<RoomEvent>,
}
```

每房间 broadcast capacity 固定为 256；不存在的房间在首次 subscribe/play 时建立。

- [ ] **Step 4: 实现精确 public API**

```rust
impl LiveHub {
    pub fn new() -> Self;
    pub async fn viewer_count(&self, stream_id: &str) -> usize;
    pub async fn viewer_counts(&self, stream_ids: &[String]) -> HashMap<String, usize>;
    pub async fn play(&self, stream_id: &str, client_id: &str, viewer_key: &str) -> usize;
    pub async fn stop(&self, stream_id: &str, client_id: &str) -> usize;
    pub async fn clear_stream(&self, stream_id: &str) -> usize;
    pub async fn subscribe(&self, stream_id: &str) -> (usize, broadcast::Receiver<RoomEvent>);
    pub async fn broadcast_danmaku(&self, stream_id: &str, event: RoomEvent);
}
```

`play`/`stop` 只在人数字面值变化时广播 `viewer_count`；`clear_stream` 总是广播 0。发送失败表示当前无 WS 接收者，不得影响 presence。

- [ ] **Step 5: 把 LiveHub 注入 AppState**

`AppState` 增加 `pub live_hub: Arc<LiveHub>`；`main` 启动时创建一次 `Arc::new(LiveHub::new())`。两个现有测试 `AppState` 构造器都补同样字段，不能使用全局 static。

- [ ] **Step 6: 运行 GREEN 与状态构造回归**

Run: `cargo test --locked live_hub::tests`

Expected: 全部 PASS。

Run: `cargo test --locked handlers::playback::tests`

Expected: 2 个协议测试 PASS。

Run: `cargo test --locked handlers::srs_callback::tests`

Expected: 现有 callback 测试全部 PASS。

- [ ] **Step 7: 记录提交边界（BLOCKED pending explicit user permission）**

建议 semantic commit：`feat(api): add in-memory live room hub`。不要执行 git 写命令。

---

### Task 4: 公开注册返回 JWT 且仅 bootstrap 创建房间

**Files:**
- Modify/Test: `api_rs/src/handlers/account.rs:56-152`

- [ ] **Step 1: 写注册模式 RED 测试**

把角色/建房判断提取为纯函数并测试：`existing_users == 0` 返回 `(super_admin, true)`；`existing_users >= 1` 返回 `(user, false)`。再写 handler 测试断言成功注册响应 `data.token` 非空。

- [ ] **Step 2: 运行 RED 测试**

Run: `cargo test --locked handlers::account::tests`

Expected: FAIL，当前注册响应 `data` 为 null，且后续用户仍无条件插入默认房间。

- [ ] **Step 3: 统一 token 响应 DTO**

把当前私有 `LoginResponse` 重命名为 `AuthTokenResponse { token: String }`，让 `create`、`login`、`refresh` 都返回同一 JSON 形状。

- [ ] **Step 4: 只为首账号插入默认房间**

保留用户表兼容 `stream_code` 生成；只有 `existing_users == 0` 时才在事务中插入 `stream_id = username` 的默认房间。后续公开注册跳过 room insert，但仍提交用户事务。

- [ ] **Step 5: 注册成功立即签发账号 JWT**

事务 commit 后调用现有：

```rust
create_jwt(
    created_user.id,
    &created_user.username,
    &created_user.role,
    &state.config.user.auth_secret,
)
```

成功返回 `success_response(AuthTokenResponse { token })`；签发失败返回 HTTP 500，日志只记录 jsonwebtoken 错误，不记录密码/JWT。

- [ ] **Step 6: 验证管理员创建用户相邻行为不变**

不得修改 `handlers/admin.rs::create_user` 的默认房间逻辑；管理员创建用户仍返回 `room_count: 1`。

- [ ] **Step 7: 运行 GREEN**

Run: `cargo test --locked handlers::account::tests`

Expected: bootstrap、后续注册、token 形状测试 PASS。

Run: `cargo test --locked handlers::admin`

Expected: 退出码 0，管理员模块编译且既有行为不回归。

- [ ] **Step 8: 记录提交边界（BLOCKED pending explicit user permission）**

建议 semantic commit：`feat(api): separate viewer registration from room creation`。不要执行 git 写命令。

---

### Task 5: 公共房间元数据、取票路由与安全 tracing

**Files:**
- Create/Test: `api_rs/src/handlers/room.rs`
- Modify: `api_rs/src/handlers/mod.rs:1-8`
- Modify: `api_rs/src/main.rs:9-34,250-263,425-433`
- Modify/Test: `api_rs/src/handlers/live.rs:34-62,849-958,1006-1225`

- [ ] **Step 1: 写公共 DTO 与列表 RED 测试**

扩展 `build_public_live_rooms` 测试，传入 hub count map 后断言结果包含 `viewer_count`、`require_login`、`has_password`，并断言即使 flags 为 true 仍出现在 live list。测试不得读取 `SrsStream.clients` 作为 expected count。

- [ ] **Step 2: 写取票 handler RED 测试**

使用 SeaORM MockDatabase 覆盖：未知房间 404；公共房间游客成功；登录房间无 JWT 401；密码房间缺失/错误密码 403；malformed guest UUID 400；有效已登录用户使用 `user:<id>` 且忽略 malformed guest ID；停用账号 401。

- [ ] **Step 3: 运行 RED 测试**

Run: `cargo test --locked handlers::room::tests`

Expected: FAIL，`handlers::room` 尚未声明。

- [ ] **Step 4: 定义公共 handler DTO**

`handlers/room.rs` 定义：

```rust
#[derive(Serialize)]
pub struct PublicRoomMetadata {
    pub stream_id: String,
    pub title: String,
    pub cover_url: String,
    pub status: String,
    pub require_login: bool,
    pub has_password: bool,
    pub viewer_count: usize,
}

#[derive(Deserialize)]
pub struct RoomAccessRequest {
    pub guest_id: String,
    #[serde(default)]
    pub password: Option<String>,
}

#[derive(Serialize)]
pub struct RoomAccessResponse {
    pub ticket: String,
    pub expires_at: String,
    pub viewer: ViewerIdentity,
}
```

- [ ] **Step 5: 实现 `GET /api/live/rooms/:stream_id`**

`pub async fn metadata(State, Path<String>)` 查询 `live_room` 和 active `live_session`；标题为空时回退 stream ID；`has_password = !password_hash.is_empty()`；viewer count 来自 `state.live_hub.viewer_count()`。未知房间 404，数据库失败 500，响应绝不序列化 Model。

- [ ] **Step 6: 实现可选 Bearer 账号解析**

在取票 handler 内只读取 `Authorization: Bearer`：缺失返回 `None`；存在但格式/签名错误返回 401；有效 claims 必须再次按 ID 查 `user` 并要求 enabled，随后用数据库 username/role 构造 `CurrentUser`。不得把仅解码的 claims 当作当前账号。

- [ ] **Step 7: 实现 `POST /access`**

执行顺序固定为：加载 enabled 房间；解析可选账号；账号存在时身份为 `user:<id>`/数据库 username；账号缺失时规范化 guest UUID 并生成 `guest:<uuid>`/游客名；仅在房间有密码时校验输入；调用 `evaluate_room_policy`；调用 `issue_room_ticket`。明文密码只活在当前请求栈中。

- [ ] **Step 8: 扩展公共 live list**

`PublicLiveRoom` 增加：

```rust
pub require_login: bool,
pub has_password: bool,
pub viewer_count: usize,
```

`public_live_rooms` 在拿到正在直播 stream IDs 后一次调用 `viewer_counts`；`build_public_live_rooms` 从房间模型派生 flags，从 count map 读取人数，默认 0。保留现有 SRS online 过滤、排序、码率和分辨率逻辑。

- [ ] **Step 9: 注册路由**

公共 Router 增加：

```rust
.route("/api/live/rooms/:stream_id", get(handlers::room::metadata))
.route("/api/live/rooms/:stream_id/access", post(handlers::room::access))
```

`handlers/mod.rs` 导出 `pub mod room;`。

- [ ] **Step 10: HTTP tracing 只记录 `MatchedPath` 路由模板**

`deploy/srs/srs.conf` 的 heartbeat 为 `/api/internal/srs/heartbeat/<callback_secret>`，所以即使只记录 raw path 也会泄密。`main.rs` 导入 `axum::extract::MatchedPath`，把默认 `TraceLayer::new_for_http()` 改为自定义 span；可编译方向固定为：

```rust
use axum::extract::MatchedPath;
use tower_http::trace::TraceLayer;
use tracing::info_span;

let trace_layer = TraceLayer::new_for_http().make_span_with(|request: &axum::http::Request<_>| {
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .unwrap_or("unmatched");
    info_span!(
        "http_request",
        method = %request.method(),
        route = route,
    )
});
```

已匹配 heartbeat 只能记录模板 `/api/internal/srs/heartbeat/:callback_secret`；没有 `MatchedPath` 时只记录固定值 `"unmatched"`。任何分支都禁止 fallback 到 `request.uri()`、`request.uri().path()`、query、headers 或 body。保留 response status/latency tracing；Task 15 用真实 heartbeat 和 `$callbackSecret` 日志扫描验证该约束。

- [ ] **Step 11: 运行 GREEN 与相邻回归**

Run: `cargo test --locked handlers::room::tests`

Expected: 元数据、guest/account 取票和错误状态测试全部 PASS。

Run: `cargo test --locked handlers::live::tests`

Expected: 既有 live tests 与新增 flags/count tests 全部 PASS。

- [ ] **Step 12: 记录提交边界（BLOCKED pending explicit user permission）**

建议 semantic commit：`feat(api): expose room metadata and access tickets`。不要执行 git 写命令。

---

### Task 6: 房主与管理员隐私更新 API

**Files:**
- Create/Test: `api_rs/src/room_privacy.rs`
- Modify/Test: `api_rs/src/handlers/room.rs`
- Modify/Test: `api_rs/src/handlers/admin.rs:42-98,214-249,665-910`
- Modify: `api_rs/src/handlers/live.rs:49-62,85-118`
- Modify: `api_rs/src/main.rs:325-404`

- [ ] **Step 1: 写 handler 权限和隐私更新 RED 测试**

在 `handlers/room.rs` 与 `handlers/admin.rs` 覆盖：非房主 403；房主成功；普通 admin 可更新任意房间隐私；普通 admin 仍不能更新 `user_id`/`stream_id`/`enabled`；创建密码房间未给 6-64 字符密码返回 400；空密码保持现有 hash；关闭清 hash；无实际变化 revision 不变。MockDatabase 仅用于 HTTP 权限/错误映射和纯 `prepare_privacy_update` 结果，测试名不得声称验证 transaction、row lock 或并发串行化。

- [ ] **Step 2: 写真实 PostgreSQL 锁与 ticket 失效 RED 测试**

在 `room_privacy.rs` 增加 `#[ignore = "requires PostgreSQL"]` 测试，读取 `YANTUBE_TEST_DATABASE_URL`，为每个测试插入唯一 username/stream_id fixture，并在 `finally` 等价的 Rust guard/显式 cleanup 中删除 fixture：

```rust
#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn concurrent_privacy_updates_are_serialized() {
    // revision=N；Barrier 同时释放两个独立连接上的 update_room_with_privacy_locked：
    // A => require_login=true,password_enabled=false
    // B => require_login=true,password_enabled=true,password=Some("concurrent-pass")
    // 两个响应都成功，最终重新 SELECT 得到 access_revision=N+2。
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn each_committed_privacy_change_stales_the_previous_ticket() {
    // 签发 ticket@N；提交设置 A 后 admit => StalePolicy；
    // 签发 ticket@N+1；提交设置 B 后 admit => StalePolicy；最终 revision=N+2。
}
```

并发测试必须使用两个 clone 的 `DatabaseConnection` 和 `tokio::sync::Barrier`，不得改写成顺序调用；只有这个真实 PostgreSQL 测试与 Task 15 的并发 HTTP 请求可以作为行锁证据。

- [ ] **Step 3: 运行 RED 测试**

Run: `cargo test --locked room_access::tests::privacy`

Expected: 纯 `prepare_privacy_update` 策略测试 PASS。

Run: `cargo test --locked handlers::room::tests::privacy`

Expected: FAIL，因为 owner handler 尚未调用 transaction helper。

Run: `cargo test --locked handlers::admin::tests::privacy`

Expected: FAIL，因为 admin handler 尚未调用同一 transaction helper。

若执行环境已有 Task 15 PostgreSQL，则运行：

```powershell
$env:YANTUBE_TEST_DATABASE_URL = 'postgres://yantube:yantube_test_password@127.0.0.1:15432/yantube'
cargo test --locked room_privacy::tests::concurrent_privacy_updates_are_serialized -- --ignored --nocapture
```

Expected: FAIL（helper 尚不存在或最终 revision 不是 `N+2`）。若 PostgreSQL 尚未启动，保留该 RED 证据到 Task 15 Step 5，不能用 MockDatabase 替代。

- [ ] **Step 4: 定义共用锁定更新契约**

`room_privacy.rs` 固定导出：

```rust
use chrono::{DateTime, Utc};
use sea_orm::{
    ActiveModelTrait, DatabaseConnection, DbErr, EntityTrait, QuerySelect,
    Set, TransactionTrait,
};
use crate::{
    entities::live_room,
    room_access::{prepare_privacy_update, RoomAccessError, RoomPrivacyInput},
};

pub enum RoomUpdateActor {
    Owner { user_id: i32 },
    Admin,
}

pub struct LockedRoomUpdate {
    pub require_login: Option<bool>,
    pub password_enabled: Option<bool>,
    pub password: Option<String>,
    pub title: Option<String>,
    pub user_id: Option<i32>,
    pub stream_id: Option<String>,
    pub enabled: Option<bool>,
}

#[derive(Debug)]
pub enum RoomPrivacyUpdateError {
    NotFound,
    Forbidden,
    Invalid(RoomAccessError),
    Database(DbErr),
}

pub async fn update_room_with_privacy_locked(
    db: &DatabaseConnection,
    room_id: i32,
    actor: RoomUpdateActor,
    patch: LockedRoomUpdate,
    now: DateTime<Utc>,
) -> Result<live_room::Model, RoomPrivacyUpdateError>;
```

`main.rs` 注册 `mod room_privacy;`。`now` 只用于一致设置 `updated_at`；密码与 revision 计算仍由 Task 2 的 `prepare_privacy_update` 完成。

- [ ] **Step 5: 实现同一 transaction 内的 lock/read/compute/update/commit**

helper 的顺序必须固定为：

```rust
let txn = db.begin().await.map_err(RoomPrivacyUpdateError::Database)?;
let result: Result<live_room::Model, RoomPrivacyUpdateError> = async {
    let locked = live_room::Entity::find_by_id(room_id)
        .lock_exclusive()
        .one(&txn)
        .await
        .map_err(RoomPrivacyUpdateError::Database)?
        .ok_or(RoomPrivacyUpdateError::NotFound)?;

    if matches!(actor, RoomUpdateActor::Owner { user_id } if locked.user_id != user_id) {
        return Err(RoomPrivacyUpdateError::Forbidden);
    }

    let input = RoomPrivacyInput {
        require_login: patch.require_login.unwrap_or(locked.require_login),
        password_enabled: patch
            .password_enabled
            .unwrap_or(!locked.password_hash.is_empty()),
        password: patch.password,
    };
    let mutation = prepare_privacy_update(&locked, input)
        .map_err(RoomPrivacyUpdateError::Invalid)?;
    let mut active: live_room::ActiveModel = locked.into();
    active.require_login = Set(mutation.require_login);
    active.password_hash = Set(mutation.password_hash);
    active.access_revision = Set(mutation.access_revision);
    active.updated_at = Set(now.naive_utc());
    if let Some(title) = patch.title { active.title = Set(title); }
    if let Some(user_id) = patch.user_id { active.user_id = Set(user_id); }
    if let Some(stream_id) = patch.stream_id { active.stream_id = Set(stream_id); }
    if let Some(enabled) = patch.enabled { active.enabled = Set(enabled); }
    active.update(&txn).await.map_err(RoomPrivacyUpdateError::Database)
}.await;

match result {
    Ok(updated) => {
        txn.commit().await.map_err(RoomPrivacyUpdateError::Database)?;
        Ok(updated)
    }
    Err(error) => {
        txn.rollback().await.map_err(RoomPrivacyUpdateError::Database)?;
        Err(error)
    }
}
```

内部 `async` result block 收集 `NotFound`、`Forbidden`、validation 和 `DbErr`：成功才 `commit`；任何 pre-commit 错误都显式 `rollback().await` 后返回原始映射错误，rollback 自身失败映射 `Database`，且日志只写固定错误分类/room id，不写 password/hash。`ActiveModel::update` 必须传 `&txn`，不得在 transaction 外预读当前 flags/revision，也不得用 `update_many` 的 `access_revision + 1` 绕过密码计算。

- [ ] **Step 6: 增加房主响应字段**

`OwnLiveRoom` 和 `own_live_room_response` 增加 `require_login`、`has_password`。`my_live_rooms`、标题/推流码更新的现有响应自然携带这两个字段，绝不携带 hash/revision。

- [ ] **Step 7: 实现房主专用路由并调用锁定 helper**

增加：

```rust
#[derive(Deserialize)]
pub struct UpdateRoomPrivacyRequest {
    pub require_login: bool,
    pub password_enabled: bool,
    #[serde(default)]
    pub password: Option<String>,
}

#[derive(Serialize)]
pub struct RoomPrivacyResponse {
    pub require_login: bool,
    pub has_password: bool,
}

pub async fn update_owned_privacy(
    State(state): State<Arc<AppState>>,
    auth_user: CurrentUser,
    Path(id): Path<i32>,
    Json(req): Json<UpdateRoomPrivacyRequest>,
) -> impl IntoResponse;
```

不要预读房间。直接把完整两个 bool/password 映射为 `LockedRoomUpdate`，其余字段为 `None`，以 `RoomUpdateActor::Owner { user_id: auth_user.user_id }` 调用 `update_room_with_privacy_locked`；ownership 必须基于 transaction 内锁定后的行检查。错误映射固定为 `NotFound -> 404`、`Forbidden -> 403`、`Invalid(MalformedPassword) -> 400`、`Database/Internal -> 500`；响应仍只返回两个 flags。

- [ ] **Step 8: 注册 owner route**

在 JWT protected Router 增加：

```rust
.route(
    "/api/live/rooms/:id/privacy",
    put(handlers::room::update_owned_privacy),
)
```

- [ ] **Step 9: 扩展管理员请求与响应**

`AdminRoomResponse` 增加 `require_login`、`has_password`。`CreateRoomRequest` 增加 `#[serde(default)] require_login: bool`、`#[serde(default)] password_enabled: bool`、`password: Option<String>`；`UpdateRoomRequest` 增加三个 `Option` 字段。

- [ ] **Step 10: 管理员创建接入隐私计算**

创建路径无需 row lock：插入前用一个默认公开的临时状态调用同一密码长度/enablement 规则，随后给 ActiveModel 显式设置 `require_login`、`password_hash`、`access_revision: 0`。如果创建时启用任何隐私开关，revision 仍从 0 起，因为此前不存在可失效 ticket。

- [ ] **Step 11: 管理员更新复用同一锁定 helper**

`super_only_change` 只包含 `user_id`、`stream_id`、`enabled`；隐私字段和 title 对普通 admin 开放。若三个隐私字段任一出现，handler 不得先查当前 room 来补 bool；它先执行现有 role/request 级权限校验，再把仍为 `Option` 的隐私字段及同 request 的 title/user_id/stream_id/enabled 放进 `LockedRoomUpdate`，以 `RoomUpdateActor::Admin` 调用 `update_room_with_privacy_locked`。省略 bool 必须由 helper 在锁定后的 model 上补齐，所有字段由同一个 `ActiveModel::update(&txn)` 落库。完全不含隐私字段的旧更新可保留现有路径；但 owner 与 admin 的每条隐私变更路径都只能经过这一 helper。

- [ ] **Step 12: 运行 GREEN 与权限回归**

Run: `cargo test --locked room_access::tests`

Expected: privacy mutation 全部 PASS。

Run: `cargo test --locked handlers::room::tests`

Expected: owner/非 owner 状态码测试 PASS。

Run: `cargo test --locked handlers::admin`

Expected: 退出码 0；admin/super_admin 现有权限代码编译通过。

Run: `cargo test --locked room_privacy::tests -- --ignored`

Expected: 仅在 `YANTUBE_TEST_DATABASE_URL` 指向真实 PostgreSQL 时，两个锁/失效测试 PASS；没有该环境时不声称并发已验证，留给 Task 15 Step 5。

- [ ] **Step 13: 记录提交边界（BLOCKED pending explicit user permission）**

建议 semantic commit：`feat(api): add owner and admin room privacy controls`。不要执行 git 写命令。

---

### Task 7: SRS 播放接纳与幂等观众计数

**Files:**
- Modify/Test: `api_rs/src/handlers/srs_callback.rs:58-77,151-179,323-465,497-563,620-912`

- [ ] **Step 1: 写 callback RED 测试**

使用固定 room ticket、MockDatabase 和真实 `LiveHub` 覆盖：有效 ticket `code:0`；缺失/错误签名/过期/跨房间/stale ticket `code:1`；有效回调后 hub 为 1；重复同 client 为 1；同身份第二 client 仍 1；另一身份为 2；stop 最后会话后才下降；未知/重复 stop 不降；unpublish 清零。

- [ ] **Step 2: 运行 RED 测试**

Run: `cargo test --locked handlers::srs_callback::tests::on_play`

Expected: FAIL，当前 `on_play` 无条件返回 code 0。

- [ ] **Step 3: 增加专用 ticket 参数解析**

保留发布使用的 `parse_token_from_param`；新增 `parse_room_ticket_from_param(&str) -> Option<String>`，只接受 query key `ticket`，不把 `token`、SRT streamid 或账号 JWT 当播放 ticket。

- [ ] **Step 4: 实现 `on_play` 接纳**

校验 `stream`、`client_id`、ticket 非空；按 stream ID 查 enabled `live_room`；调用 `admit_room_ticket(ticket, body.stream, room, secret, Utc::now())`；成功后调用 `live_hub.play(stream, client_id, claims.viewer_key)`。任何失败都返回 HTTP 200、`{"code":1}`。

- [ ] **Step 5: 实现 `on_stop` 幂等移除**

非空 stream/client 调用 `live_hub.stop`；空或未知 client 不改变状态；callback 始终返回 HTTP 200、code 0，保证 SRS 重试安全。

- [ ] **Step 6: `on_unpublish` 清除 presence**

无论 live_session 数据库更新是否成功，都调用 `live_hub.clear_stream(&body.stream).await` 并广播 0；原直播 episode/state 逻辑保持不变。

- [ ] **Step 7: 清理 credential 日志**

删除当前 `on_publish` invalid 分支中的 `stream_code = %stream_code` 字段。`on_play` 失败日志只允许 `stream` 和固定分类 `reason = "room access denied"`，不得输出 `body.param`、ticket、JWT、hash 或明文密码。

- [ ] **Step 8: 运行 GREEN 与 callback 回归**

Run: `cargo test --locked handlers::srs_callback::tests`

Expected: 新增接纳/计数测试与现有 publish/forward/heartbeat/reconnect 测试全部 PASS。

Run: `cargo test --locked live_hub::tests`

Expected: 全部 PASS。

- [ ] **Step 9: 记录提交边界（BLOCKED pending explicit user permission）**

建议 semantic commit：`feat(api): authorize playback and count unique viewers`。不要执行 git 写命令。

---

### Task 8: WebSocket 弹幕与实时 viewer_count

**Files:**
- Modify: `api_rs/Cargo.toml`
- Modify generated lock: `api_rs/Cargo.lock`
- Create/Test: `api_rs/src/danmaku.rs`
- Modify/Test: `api_rs/src/handlers/room.rs`
- Modify: `api_rs/src/main.rs:1-7,250-263`

- [ ] **Step 1: 写弹幕领域 RED 测试**

覆盖 guest/account sender 派生、全空白、101 Unicode scalar、`<b>literal</b>` 原样保留、服务端 ID/时间覆盖客户端字段、首条成功、1 秒内第二条 `rate_limited`、无效消息不消耗限流额度。

- [ ] **Step 2: 运行 RED 测试**

Run: `cargo test --locked danmaku::tests`

Expected: FAIL，模块尚未声明。

- [ ] **Step 3: 启用 axum WebSocket**

把 dependency 改为：

```toml
axum = { version = "0.7", features = ["multipart", "ws"] }
```

运行 cargo 命令生成必要 lockfile 变化；不加入独立 WebSocket 服务、Redis 或消息存储依赖。

- [ ] **Step 4: 定义客户端消息和连接限流器**

`danmaku.rs` 定义：

```rust
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    SendMessage { content: String },
}

pub enum DanmakuError {
    InvalidMessage,
    RateLimited,
}

pub struct ConnectionRateLimiter {
    last_accepted: Option<Instant>,
}
```

实现 `accept_message(claims, content, now, id, sent_at) -> Result<RoomEvent, DanmakuError>`；先 trim/长度检查，再检查距离 `last_accepted` 是否至少 1 秒，只有成功才更新 timestamp。

- [ ] **Step 5: 定义 WS query 和 handler**

```rust
#[derive(Deserialize)]
pub struct RoomWsQuery {
    #[serde(default)]
    pub ticket: String,
}

pub async fn websocket(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    Path(stream_id): Path<String>,
    Query(query): Query<RoomWsQuery>,
) -> Response;
```

route 为 `GET /api/live/rooms/:stream_id/ws`。ticket 不得复制到 tracing span。

- [ ] **Step 6: 在 socket task 内执行第二次接纳**

升级后首先重新查询当前 room 并调用 `admit_room_ticket`。失败时只发送 close frame `1008 / room access denied` 并返回；成功后调用 `live_hub.subscribe`，立即单播当前 `viewer_count`，这个 subscribe 不调用 `play`。

- [ ] **Step 7: 实现双向循环**

用 `tokio::select!` 同时等待 socket receive 和 broadcast receiver：文本消息反序列化并交给 limiter；成功弹幕通过 `live_hub.broadcast_danmaku`；本连接错误只单播 `error`；binary/无效 JSON 返回 `invalid_message`；`broadcast::RecvError::Lagged(_)` 继续等待未来事件；Closed/连接错误退出。

- [ ] **Step 8: 注册模块和路由**

`main.rs` 增加 `mod danmaku;`，公共 Router 增加：

```rust
.route(
    "/api/live/rooms/:stream_id/ws",
    get(handlers::room::websocket),
)
```

- [ ] **Step 9: 运行 GREEN 与安全回归**

Run: `cargo test --locked danmaku::tests`

Expected: 全部 PASS。

Run: `cargo test --locked handlers::room::tests`

Expected: admission helper、初始 count 和 invalid ticket close 构造测试 PASS。

Run: `cargo test --locked`

Expected: 全部 backend tests PASS。

- [ ] **Step 10: 记录提交边界（BLOCKED pending explicit user permission）**

建议 semantic commit：`feat(api): add live room websocket danmaku`。不要执行 git 写命令。

---

### Task 9: 前端纯函数、API 类型和 ticket URL 契约

**Files:**
- Create/Test: `front/src/libs/viewerIdentity.ts`
- Create/Test: `front/src/libs/viewerIdentity.test.ts`
- Modify/Test: `front/src/libs/streamUrls.ts`
- Modify/Test: `front/src/libs/streamUrls.test.ts`
- Modify: `front/src/libs/api.ts`
- Modify: `front/src/components/player/playerSources.ts`

- [ ] **Step 1: 写 URL RED 测试**

把预期固定为：

```text
WHEP: https://live.example/rtc/v1/whep/?app=live&stream=room-1&ticket=a%2Bb%2Fc%3D
HLS:  /live/room-1.m3u8?ticket=a%2Bb%2Fc%3D
FLV:  /live/room-1.flv?ticket=a%2Bb%2Fc%3D
WS:   wss://live.example/api/live/rooms/room-1/ws?ticket=a%2Bb%2Fc%3D
```

保留现有 RTMP/WHIP/SRT publish 断言不变。

- [ ] **Step 2: 写 guest/redirect RED 测试**

用注入式内存 Storage 和固定 UUID 工厂断言：首次生成并写入、再次复用、非法存量替换；redirect 接受 `/live/room-1?x=1#chat` 和 `/admin`，拒绝 `https://evil.test`、`//evil.test`、`javascript:alert(1)`、反斜线路径和空值并回退 `/`。

- [ ] **Step 3: 运行 RED 测试模块**

从 `front/` 运行以下 PowerShell；只把临时 JS 写到 OS temp：

```powershell
$unitOut = Join-Path $env:TEMP 'yantube-front-unit'
if (Test-Path -LiteralPath $unitOut) { Remove-Item -LiteralPath $unitOut -Recurse -Force }
pnpm exec tsc --target ES2022 --module NodeNext --moduleResolution NodeNext --lib ES2022,DOM,DOM.Iterable --skipLibCheck --noEmit false --declaration false --declarationMap false --sourceMap false --outDir $unitOut src/libs/streamUrls.ts src/libs/streamUrls.test.ts src/libs/viewerIdentity.ts src/libs/viewerIdentity.test.ts
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
node (Join-Path $unitOut 'streamUrls.test.js')
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
node (Join-Path $unitOut 'viewerIdentity.test.js')
```

Expected: 至少一个 URL/guest/redirect assertion 抛错并使进程非 0。

- [ ] **Step 4: 实现 guest 和 redirect 工具**

固定 localStorage key 为 `yantube_guest_id`，浏览器 UUID 来源只用 `crypto.randomUUID()`。导出：

```ts
export function getOrCreateGuestId(
  storage: Pick<Storage, 'getItem' | 'setItem'> = localStorage,
  randomUuid: () => string = () => crypto.randomUUID()
): string

export function sanitizeRedirect(value: string | null, origin = window.location.origin): string
export function accountValidationError(username: string, password: string): string | null
export function unicodeLength(value: string): number
```

账号校验镜像当前 Rust API：username UTF-8 字节 3-32、每个字符为 Unicode 字母/数字或 `_`，password UTF-8 字节至少 6。

- [ ] **Step 5: 实现 ticket URL builders**

`buildWhepUrl(base, roomId, ticket)` 将 query key 从 `token` 改为 `ticket`；`buildHlsPlaybackUrl(roomId, ticket)`、`buildFlvPlaybackUrl(roomId, ticket)` 都用 `URLSearchParams` 编码；新增 `buildRoomWebSocketUrl(apiBase, roomId, ticket)`，把 `http:`/`https:` 转为 `ws:`/`wss:`。

- [ ] **Step 6: 更新 player source 参数名称**

`BuildMoyuPlayerSourcesOptions` 把 `token` 改名 `ticket`，三种 source 都传 ticket；此任务先保持 `MoyuPlayer` 现有调用可编译，Task 11 移除其 JWT 来源。

- [ ] **Step 7: 增加前端 API DTO**

在 `api.ts` 定义：

```ts
export type ViewerKind = 'user' | 'guest'
export interface ViewerIdentity { kind: ViewerKind; name: string }
export interface PublicRoomMetadata {
  stream_id: string
  title: string
  cover_url: string
  status: 'live' | 'offline'
  require_login: boolean
  has_password: boolean
  viewer_count: number
}
export interface RoomAccessResult {
  ticket: string
  expires_at: string
  viewer: ViewerIdentity
}
export type RoomServerMessage =
  | { type: 'viewer_count'; count: number }
  | { type: 'danmaku'; id: string; sender: ViewerIdentity; content: string; sent_at: string }
  | { type: 'error'; code: 'rate_limited' | 'invalid_message'; message: string }
export type DanmakuMessage = Extract<RoomServerMessage, { type: 'danmaku' }>
```

- [ ] **Step 8: 增加 API 函数和 DTO 字段**

实现 `register(LoginParams)`、`logout()`、`getPublicRoom(streamId)`、`requestRoomAccess(streamId,{guest_id,password?})`、`updateOwnedRoomPrivacy(id,input)`。`LiveRoom` 增加 flags/count；`OwnLiveRoom`、`AdminRoom` 增加 flags；admin create/update params 增加 `require_login`、`password_enabled`、`password?`。给内部 `request` 增加 `refreshOn401?: boolean`（默认 true），`requestRoomAccess` 固定传 false，避免公共房间因过期账号 JWT 被全局跳去登录；非 2xx 使用下列类型，供房间状态机精确分支：

```ts
export class ApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly code: number,
    message: string
  ) {
    super(message)
    this.name = 'ApiError'
  }
}
```

`request` 在任何 HTTP response 上先解析统一 envelope；`refreshOn401:false` 时直接抛当前 response 的 `ApiError`，不得 refresh 或写 `window.location.href`。

- [ ] **Step 9: 运行 GREEN**

重复 Step 3 命令。

Expected: 两个 node 进程退出码 0。

Run: `pnpm build`

Expected: TypeScript 和 Vite production build PASS。

- [ ] **Step 10: 记录提交边界（BLOCKED pending explicit user permission）**

建议 semantic commit：`feat(front): add viewer identity and room ticket contracts`。不要执行 git 写命令。

---

### Task 10: 共用账号动作与登录/注册流程

**Files:**
- Create: `front/src/components/AccountActions.tsx`
- Modify: `front/src/pages/Login.tsx`
- Modify: `front/src/pages/Home.tsx`
- Modify: `front/src/components/ProtectedRoute.tsx`

- [ ] **Step 1: 先加入表单与 redirect 失败断言**

在 `viewerIdentity.test.ts` 增加 username/password 边界断言和 percent-encoded 外站 redirect 拒绝断言，再用 Task 9 的临时编译命令运行。

Expected: 新断言在 helper 未覆盖时 FAIL。

- [ ] **Step 2: 完成校验 helper 并确认 RED 转 GREEN**

补足 UTF-8 字节长度和 Unicode 字母数字判定。

Run: Task 9 Step 3 的 PowerShell 命令。

Expected: `viewerIdentity.test.js` 退出码 0。

- [ ] **Step 3: 实现 `AccountActions`**

组件用 `decodeToken()` 显示两种明确状态：未登录显示“登录 / 注册”链接；已登录显示用户名、`admin/super_admin` 的“管理后台”或普通用户的“推流管理”、以及退出按钮。退出先 best-effort 调用 API `logout()`，再 `clearToken()` 并导航到 `/`；按钮保留可见 focus。

- [ ] **Step 4: 把 Login 改成双模式**

同一 Card 用 tabs 或按钮在“登录”“注册”间切换；两种模式共用 username/password、`accountValidationError`、loading 和 Alert。登录调用 `login`，注册调用 `register`，两者成功都写 `jwt`。

- [ ] **Step 5: 实现安全返回路径**

读取 `useSearchParams().get('redirect')`，只用 `sanitizeRedirect` 的结果导航；没有有效参数时回 `/`。页面标题改为通用账号文案，不再写“管理后台登录”。

- [ ] **Step 6: ProtectedRoute 保留当前路径**

使用 `useLocation` 拼出 `pathname + search + hash`，通过 `encodeURIComponent` 生成例如 `/login?redirect=%2Fadmin`；登录后可回 `/admin`，不得把外部 URL 写入 redirect。

- [ ] **Step 7: 首页挂载共用动作**

在 Home 顶部标题/action 区加入 `<AccountActions />`，375px 时和直播/RSS chips 纵向或换行，不产生水平滚动；遵循 `front/DESIGN.md` 的 MUI dark、border、4/8px spacing。

- [ ] **Step 8: 运行前端门禁**

Run: `pnpm lint`

Expected: 0 warnings、退出码 0。

Run: `pnpm build`

Expected: production build PASS。

- [ ] **Step 9: 记录提交边界（BLOCKED pending explicit user permission）**

建议 semantic commit：`feat(front): add viewer login and registration`。不要执行 git 写命令。

---

### Task 11: Player 只接收 room ticket 并渲染弹幕层

**Files:**
- Create: `front/src/components/player/DanmakuOverlay.tsx`
- Create: `front/src/components/player/DanmakuOverlay.module.scss`
- Modify: `front/src/components/player/MoyuPlayer.tsx:136-172,243-297,461-622`
- Modify: `front/src/components/player/playerSources.ts`
- Modify: `front/src/components/player/playbackAdapters.ts:12-42,52-110`
- Modify: `front/src/components/player/MoyuPlayer.module.scss`
- Modify transitional caller: `front/src/pages/AdminStreamCode.tsx:521-528`

- [ ] **Step 1: 扩充 URL RED 断言**

在 `streamUrls.test.ts` 增加 ticket 含 `+ / = ? &` 时三种媒体 URL 都只出现一个正确编码的 `ticket` key，并断言没有 `token=`。

- [ ] **Step 2: 运行 RED URL 测试**

Run: Task 9 Step 3 的 PowerShell 命令。

Expected: 若任一 builder 仍用 token 或漏编码则 FAIL。

- [ ] **Step 3: 改 `MoyuPlayerProps`**

固定 props：

```ts
export interface MoyuPlayerProps {
  roomId: string
  ticket: string
  danmakuMessages?: readonly DanmakuMessage[]
  onVideoElementChange?: (video: HTMLVideoElement | null) => void
}
```

删除 `localStorage.getItem('jwt')`；source memo 和 attach effect 只依赖 `ticket`。ticket 为空时不 attach source。

- [ ] **Step 4: 移除 WHEP Bearer**

`PlaybackAdapterOptions` 删除 token；`attachWebRtc` 调用固定为 `whep.view(pc, url, undefined, abortController.signal)`。URL 中仍有 room ticket，Authorization header 不再出现。

- [ ] **Step 5: 实现弹幕 overlay 组件**

`DanmakuMessage` 使用服务端事件字段。overlay 对最近进入的消息分配有限轨道，key 使用服务端 ID；动画层 `aria-hidden="true"`，容器 `pointer-events:none`、`overflow:hidden`，bottom 留出 player controls 高度。

- [ ] **Step 6: 实现 reduced-motion**

正常动画 8-12 秒 linear，只变 `transform`/`opacity`；`@media (prefers-reduced-motion: reduce)` 禁用移动并让静态副本显示 6 秒。不得覆盖 controls 或拦截视频/控制栏点击。

- [ ] **Step 7: 临时保护管理预览调用点**

`AdminStreamCode` 在没有 preview room ticket 时不挂载 `MoyuPlayer`，显示明确 Alert；Task 14 将接入真实预览取票。禁止用账号 JWT 填充 `ticket` prop。

- [ ] **Step 8: 运行 GREEN 与播放器回归**

Run: Task 9 Step 3 的 PowerShell 命令。

Expected: URL tests PASS。

Run: `pnpm lint`

Expected: 0 warnings。

Run: `pnpm build`

Expected: production build PASS；现有 fullscreen 类型测试仍被 TypeScript 检查。

- [ ] **Step 9: 记录提交边界（BLOCKED pending explicit user permission）**

建议 semantic commit：`feat(front): secure player with room tickets and danmaku overlay`。不要执行 git 写命令。

---

### Task 12: 房间访问状态机、WebSocket 重连与弹幕界面

**Files:**
- Create/Test: `front/src/libs/roomAccessState.ts`
- Create/Test: `front/src/libs/roomAccessState.test.ts`
- Create: `front/src/hooks/useRoomChannel.ts`
- Create: `front/src/components/room/RoomAccessGate.tsx`
- Create: `front/src/components/room/DanmakuPanel.tsx`
- Modify: `front/src/pages/Room.tsx`

- [ ] **Step 1: 写 stale metadata 与恢复预算 RED 测试**

`roomAccessState.test.ts` 用内建 assertion helper 固定四个场景；核心断言写成：

```ts
import type { PublicRoomMetadata } from './api'
import {
  consumeAccessAttempt,
  consumeMetadataRefresh,
  gateFromFreshMetadata,
  newRecoveryBudget,
} from './roomAccessState'

function assertJson(actual: unknown, expected: unknown, label: string) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${label}: ${JSON.stringify(actual)}`)
  }
}

const metadata = (require_login: boolean, has_password: boolean): PublicRoomMetadata => ({
  stream_id: 'room-1', title: 'Room', cover_url: '', status: 'live',
  require_login, has_password, viewer_count: 0,
})

assertJson(
  gateFromFreshMetadata(metadata(true, false), false, false, 'access_401'),
  { kind: 'login_required', clearPassword: false },
  'public to login'
)
assertJson(
  gateFromFreshMetadata(metadata(false, true), false, false, 'access_403'),
  { kind: 'password_required', clearPassword: true },
  'public to password'
)
assertJson(
  gateFromFreshMetadata(metadata(false, true), false, true, 'access_403'),
  { kind: 'password_required', clearPassword: true },
  'password change clears stale password'
)

const firstCycle = newRecoveryBudget()
const afterMetadata = consumeMetadataRefresh(firstCycle)
if (!afterMetadata) throw new Error('first metadata refresh denied')
if (consumeMetadataRefresh(afterMetadata) !== null) throw new Error('second metadata refresh allowed')
const afterAccess = consumeAccessAttempt(afterMetadata)
if (!afterAccess) throw new Error('first access attempt denied')
if (consumeAccessAttempt(afterAccess) !== null) throw new Error('second access attempt allowed')
const userRetryCycle = newRecoveryBudget()
assertJson(userRetryCycle, { metadataRefreshes: 0, accessAttempts: 0 }, 'user retry resets cycle')
```

测试必须断言计数 `metadataRefreshes === 1`、`accessAttempts <= 1`；不只断言最终文案。

- [ ] **Step 2: 运行恢复状态 RED 测试**

从 `front/`：

```powershell
$roomStateOut = Join-Path $env:TEMP 'yantube-room-state-unit'
if (Test-Path -LiteralPath $roomStateOut) { Remove-Item -LiteralPath $roomStateOut -Recurse -Force }
pnpm exec tsc --target ES2022 --module NodeNext --moduleResolution NodeNext --lib ES2022,DOM,DOM.Iterable --skipLibCheck --noEmit false --declaration false --declarationMap false --sourceMap false --outDir $roomStateOut src/libs/roomAccessState.ts src/libs/roomAccessState.test.ts
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
node (Join-Path $roomStateOut 'roomAccessState.test.js')
```

Expected: FAIL，因为恢复 budget/gate reducer 尚不存在。

- [ ] **Step 3: 定义纯状态与 hook 状态契约**

`roomAccessState.ts` 导出：

```ts
import type { PublicRoomMetadata } from './api'

export type RecoveryTrigger =
  | 'access_401'
  | 'access_403'
  | 'ws_1008'
  | 'reacquire_failed'

export interface RecoveryBudget {
  metadataRefreshes: 0 | 1
  accessAttempts: 0 | 1
}

export type FreshMetadataDecision =
  | { kind: 'login_required'; clearPassword: false }
  | { kind: 'password_required'; clearPassword: true }
  | { kind: 'request_access'; clearPassword: false }

export function newRecoveryBudget(): RecoveryBudget
export function consumeMetadataRefresh(budget: RecoveryBudget): RecoveryBudget | null
export function consumeAccessAttempt(budget: RecoveryBudget): RecoveryBudget | null
export function gateFromFreshMetadata(
  metadata: PublicRoomMetadata,
  hasValidAccount: boolean,
  hasPasswordInMemory: boolean,
  trigger: RecoveryTrigger
): FreshMetadataDecision
```

`consume*` 在对应计数已经为 1 时返回 `null`；`gateFromFreshMetadata` 优先 login gate，其次 password gate。`access_403`/`reacquire_failed` 遇到 `has_password` 必须返回 `password_required` 并由 hook 清空旧密码；`ws_1008` 仅在内存仍有密码时可进行一次 access。

使用 discriminated union，禁止通过多个互相矛盾的 boolean 表示 gate：

```ts
type AccessState =
  | { kind: 'loading' }
  | { kind: 'login_required'; message: string }
  | { kind: 'password_required'; message: string }
  | { kind: 'error'; message: string }
  | { kind: 'ready'; ticket: string; expiresAt: number; viewer: ViewerIdentity }

type ConnectionState = 'idle' | 'connecting' | 'connected' | 'reconnecting' | 'disconnected'
```

- [ ] **Step 4: 实现元数据和首次访问流程**

`useRoomChannel(streamId)` 先 `getPublicRoom`；登录 required 且 `decodeToken()` 为空时停在 login gate；password required 时等待用户 submit；其余场景自动用稳定 guest ID 调 `requestRoomAccess`。password state 只存在 Room/hook 内存，不能写 localStorage/sessionStorage/query。

- [ ] **Step 5: 实现强制 metadata refresh 的单周期恢复**

实现单一 `recoverAccess(trigger: RecoveryTrigger)`，所有 ticket access `401/403`、WebSocket `1008`、ticket 到期重新取票失败都只进入这里。进入时立即关闭 socket、卸载 player、清除旧 ticket，并用 `recoveryInFlightRef` 合并并发 trigger；然后严格执行：

1. 新建 `RecoveryBudget`，消费唯一一次 metadata refresh，强制重新 `GET /api/live/rooms/:stream_id`，不得根据 hook 中缓存 metadata 直接选 gate。
2. 用最新 `require_login/has_password` 调 `gateFromFreshMetadata`：login gate 先清无效 JWT；password gate 清空旧 password 并显示统一“房间密码不正确或访问已失效”；这两个 gate 都不自动 access。
3. 仅当决策为 `request_access` 时消费唯一一次 access attempt；成功进入 `ready`，失败按最新 metadata 映射为明确 login/password/error 状态，且不得递归调用 `recoverAccess`。
4. 最新 metadata 404 显示房间不存在；400 显示输入错误；latest-public 的 401 清 JWT 后停在“登录状态已失效，请重试”，latest-public 的 403 停在“访问策略已变化，请重试”。
5. 只有用户点击“重试”、提交新密码或完成登录后重新进入页面才创建新 cycle；timer、socket close 和 failed fetch 不能自动重置预算。

这保证 public→login、public→password、password change 都先获得最新 flags，且每个恢复周期最多 `1 GET metadata + 1 POST access`。player 只在 `ready` 分支挂载，避免 unauthorized 闪现。

- [ ] **Step 6: 实现 WebSocket 与 bounded backoff**

ready 后用 `buildRoomWebSocketUrl` 连接。重连延迟序列固定 `1000, 2000, 4000, 8000, 15000ms`，达到上限后持续 15 秒间隔，unmount/stream change 时取消 timer/socket。普通网络 close 走 bounded backoff；若距 `expiresAt` 少于 30 秒，先重新取票，失败进入 `recoverAccess('reacquire_failed')`；close code `1008` 直接进入 `recoverAccess('ws_1008')`。密码仍只从组件内存提供，同一恢复 cycle 内的 close/fetch error 不得排入第二个自动 timer。

- [ ] **Step 7: 实现消息处理**

`viewer_count` 替换当前人数；`danmaku` 按 ID 去重并只保留最近 100 条内存消息；`error` 只进入 composer 的当前错误，不追加弹幕。JSON parse 失败关闭当前 socket 并走退避，不渲染不可信 HTML。

- [ ] **Step 8: 实现 `RoomAccessGate`**

login gate 使用 `const redirect = encodeURIComponent(`/live/${encodeURIComponent(streamId)}`)` 生成 `/login?redirect=${redirect}`；password gate 有 label、6-64 字符 helper、submit loading 和 Alert；所有状态键盘可操作且有 visible focus。

- [ ] **Step 9: 实现 `DanmakuPanel`**

显示 ticket viewer 名、connection chip、`unicodeLength(content)/100`、TextField、send button。submit 发送唯一 JSON `{"type":"send_message","content":value}`；本地阻止 trim 后 0 或 >100；断线时 disable。最近消息区使用普通 React 文本节点和可访问 heading，不使用 `dangerouslySetInnerHTML`。

- [ ] **Step 10: 重写 Room 布局**

顶部保留返回/RSS并加入 `AccountActions`；ready 时显示标题、隐私状态、`aria-live="polite"` viewer count、`MoyuPlayer(ticket,danmakuMessages)` 和 `DanmakuPanel`。`md` 及以上 player/chat 并排，小于 `md` 单列；375px 无水平滚动，composer 不覆盖 controls。

- [ ] **Step 11: 增加稳定 QA selectors**

为 `room-access-gate`、`viewer-count`、`danmaku-composer`、`danmaku-recent`、`danmaku-overlay` 增加 `data-testid`；它们只用于自动真实表面检查，不改变可访问名称。

- [ ] **Step 12: 运行 GREEN 与前端门禁**

Run: Step 2 的 PowerShell 命令。

Expected: public→login、public→password、password change 和预算耗尽断言全部 PASS。

Run: `pnpm lint`

Expected: 0 warnings。

Run: `pnpm build`

Expected: production build PASS。

- [ ] **Step 13: 记录提交边界（BLOCKED pending explicit user permission）**

建议 semantic commit：`feat(front): add gated room viewing and live danmaku`。不要执行 git 写命令。

---

### Task 13: 首页人数、隐私状态与账号可见性

**Files:**
- Modify: `front/src/pages/Home.tsx`

- [ ] **Step 1: 给 live card 加 viewer count**

每张卡显示 `room.viewer_count`，图标/文本放在 metadata stack 中；人数区域设置 `data-testid` 为字符串模板 `viewer-count-${room.stream_id}`，轮询结果更新但不把每次变化作为 assertive announcement。

- [ ] **Step 2: 加独立隐私 chips**

`require_login` 时显示 warning semantic chip“需登录”，`has_password` 时显示 warning semantic chip“需密码”；两者同时开启时两个 chip 都显示，分别加 `data-testid="privacy-chip-login"`、`privacy-chip-password`。

- [ ] **Step 3: 保持发现与 RSS 行为**

不得在前端根据隐私 flags 过滤 room；CardActionArea 仍导航到房间 gate，RSS 按钮和 10 秒 polling 保持。

- [ ] **Step 4: 验证三档响应式**

检查 grid 在 375px 单列、768px 两列、1280px 三列；title、chips、人数允许换行且卡片 `minWidth:0`，不截断 URL 之外的普通文本。

- [ ] **Step 5: 运行门禁**

Run: `pnpm lint`

Expected: 0 warnings。

Run: `pnpm build`

Expected: PASS。

- [ ] **Step 6: 记录提交边界（BLOCKED pending explicit user permission）**

建议 semantic commit：`feat(front): show viewer counts and room privacy`。不要执行 git 写命令。

---

### Task 14: 房主/Admin 双开关 UI 与 ticket 化管理预览

**Files:**
- Create: `front/src/components/room/PrivacyControls.tsx`
- Modify: `front/src/pages/AdminStreamCode.tsx`
- Modify: `front/src/pages/AdminRooms.tsx`

- [ ] **Step 1: 实现可复用隐私表单**

props 固定包含当前 `requireLogin`、`hasPassword`、`onSave(input)` 和 loading。表单有两个独立 Switch；password switch 打开时显示 write-only password field；已有密码且输入为空明确提示“留空保持当前密码”；新启用时为空禁用保存；关闭时提交 `password_enabled:false`。

- [ ] **Step 2: 房主页面加入隐私 Card**

在选中房间的身份 Card 后增加隐私 Card，调用 `updateOwnedRoomPrivacy`；成功后只用返回 flags 更新 `rooms` 中对应元素并清空 password input。普通公开注册用户无房间时继续显示现有空态，不调用隐私 API。

- [ ] **Step 3: 管理预览获取 ticket**

选中房间变化时清除 preview ticket/password。无 password 时用当前账号 JWT 自动调用 access；有 password 时显示独立“预览密码”输入和按钮，成功取票后才挂载 `MoyuPlayer`。刚设置的新密码可复用当前组件内存值取票，但保存成功后不得持久化。

- [ ] **Step 4: AdminRooms 创建/编辑接入表单**

`RoomForm` 增加 `require_login`、`password_enabled`、`password`。create 新启用密码时要求 6-64 scalar；edit 对已有密码显示 configured 状态且空值保留。普通 admin 保存 payload 允许 `title` 和三个隐私字段；`user_id`、`stream_id`、`enabled` 仍仅 super_admin 发送。

- [ ] **Step 5: 列表显示隐私状态且不显示 hash**

房间表格用两个小 chip 显示登录/密码状态；任何 React state、TextField value、Snackbar、console 都不能从响应获得 hash。密码字段关闭 dialog 或保存后立即置空。

- [ ] **Step 6: 按 DESIGN.md 检查交互**

Card/对话框使用 divider border、MUI semantic colors、4/8px spacing；Switch/TextField 有显式 label；保存中 disable；错误用 Alert/Snackbar；375px 对话框和 Card 无横向溢出。

- [ ] **Step 7: 运行门禁**

Run: `pnpm lint`

Expected: 0 warnings。

Run: `pnpm build`

Expected: PASS；所有 `MoyuPlayer` 调用点都提供 room ticket。

- [ ] **Step 8: 记录提交边界（BLOCKED pending explicit user permission）**

建议 semantic commit：`feat(front): add room privacy management`。不要执行 git 写命令。

---

### Task 15: nginx、全量门禁、Docker 真实表面验证与清理

**Files:**
- Modify/Test: `front/nginx.conf`
- Verify only: `deploy/docker-compose.test.yml`
- Verify only: `deploy/srs/srs.conf`

- [ ] **Step 1: 配置 WebSocket Upgrade 和无 query access log**

在 nginx http context 增加：

```nginx
map $http_upgrade $connection_upgrade {
    default upgrade;
    ""      close;
}

log_format yantube_no_args '$remote_addr - $remote_user [$time_local] '
                            '"$request_method $uri $server_protocol" $status $body_bytes_sent '
                            '"$http_referer" "$http_user_agent"';
```

server 内使用 `access_log /var/log/nginx/access.log yantube_no_args;`。现有 `/api/` location 保留 limit_req/proxy headers，并增加：

```nginx
proxy_http_version 1.1;
proxy_set_header Upgrade $http_upgrade;
proxy_set_header Connection $connection_upgrade;
proxy_read_timeout 1h;
```

- [ ] **Step 2: 运行 Rust 全量门禁**

从 `api_rs/`：

```powershell
cargo fmt --all --check
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
cargo clippy --locked --all-targets -- -D warnings
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
cargo test --locked
```

Expected: 三个命令退出码均为 0；privacy matrix、ticket、registration、hub、callback、danmaku 和相邻旧测试全部 PASS。

- [ ] **Step 3: 运行前端纯函数与全量门禁**

从 `front/` 先运行 Task 9 Step 3 与 Task 12 Step 2 的临时编译/node 命令，再运行：

```powershell
pnpm lint
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
pnpm build
```

Expected: URL/guest/redirect、stale metadata 转换、单周期恢复预算 assertions、lint、TypeScript 和 Vite build 全部通过；FLV URL builder 的 ticket 编码在这里完成自动验收。

- [ ] **Step 4: 在强制清理边界内启动全新 Compose 环境并验证迁移重跑**

从 `deploy/` 执行。Step 4 到 Step 13 的 PowerShell 代码块是同一个连续脚本：本步骤打开外层 `try`，Step 13 用 `finally` 关闭。不得拆开执行或把 `throw` 改成 `exit`，否则会绕过 ffmpeg/socket/temp/Compose 清理。

```powershell
$publisher = $null
$qaSockets = [Collections.Generic.List[IDisposable]]::new()
$qaTempFiles = [Collections.Generic.List[string]]::new()
$qaUnverified = [Collections.Generic.List[string]]::new()
$qaCleanupFailures = [Collections.Generic.List[string]]::new()
$composeAttempted = $false
$hlsTicket = $null
$staleHlsTicket = $null
$postHlsGuestTicket = $null
$postHlsAccountTicket = $null

try {
$composeAttempted = $true
docker compose -f docker-compose.test.yml down -v
if ($LASTEXITCODE -ne 0) { throw "initial compose down failed with exit code $LASTEXITCODE" }
docker compose -f docker-compose.test.yml up -d --build
if ($LASTEXITCODE -ne 0) { throw "compose up failed with exit code $LASTEXITCODE" }
docker compose -f docker-compose.test.yml ps

function Wait-YantubeApi {
    $loginBody = @{ username = 'admin'; password = 'test123456' } | ConvertTo-Json -Compress
    for ($attempt = 1; $attempt -le 60; $attempt++) {
        try {
            $response = Invoke-WebRequest -Method POST -Uri 'http://127.0.0.1:9081/api/account/login' -ContentType 'application/json' -Body $loginBody -SkipHttpErrorCheck
            if ([int]$response.StatusCode -eq 200 -and ($response.Content | ConvertFrom-Json).code -eq 0) { return }
        } catch {
        }
        Start-Sleep -Seconds 1
    }
    throw 'Yantube API or seed account did not become ready in 60 seconds'
}

Wait-YantubeApi
docker compose -f docker-compose.test.yml exec -T postgres psql -U yantube -d yantube -v ON_ERROR_STOP=1 -c "SELECT require_login, password_hash, access_revision FROM live_room ORDER BY id LIMIT 1;"
if ($LASTEXITCODE -ne 0) { throw "privacy column query failed with exit code $LASTEXITCODE" }
docker compose -f docker-compose.test.yml restart api
if ($LASTEXITCODE -ne 0) { throw "api restart failed with exit code $LASTEXITCODE" }
Wait-YantubeApi
docker compose -f docker-compose.test.yml logs --no-color api
```

Expected: `postgres/api/front/srs` healthy/running，seed 完成；首房间查询为 `false`, empty, `0`；API restart 后 migration complete 且无 migration error。

- [ ] **Step 5: 运行真实 PostgreSQL 锁测试并建立 HTTP 断言 helper**

先在已启动的真实 PostgreSQL 上运行 Task 6 的 ignored tests；`Push-Location` 的 `finally` 只恢复目录，外层 Compose `try` 保持打开：

```powershell
$env:YANTUBE_TEST_DATABASE_URL = 'postgres://yantube:yantube_test_password@127.0.0.1:15432/yantube'
Push-Location '..\api_rs'
try {
    cargo test --locked room_privacy::tests::concurrent_privacy_updates_are_serialized -- --ignored --nocapture
    if ($LASTEXITCODE -ne 0) { throw "PostgreSQL concurrent privacy test failed with exit code $LASTEXITCODE" }
    cargo test --locked room_privacy::tests::each_committed_privacy_change_stales_the_previous_ticket -- --ignored --nocapture
    if ($LASTEXITCODE -ne 0) { throw "PostgreSQL ticket revision test failed with exit code $LASTEXITCODE" }
} finally {
    Pop-Location
}

$base = 'http://127.0.0.1:9081'
$callbackSecret = 'srs-test-callback-secret'
$guestId = '550e8400-e29b-41d4-a716-446655440000'
$roomPassword = 'room-pass-123'

function Invoke-JsonApi {
    param(
        [string]$Method,
        [string]$Path,
        [object]$Body = $null,
        [string]$Token = ''
    )
    $headers = @{}
    if ($Token) { $headers.Authorization = "Bearer $Token" }
    $params = @{
        Method = $Method
        Uri = "$base$Path"
        Headers = $headers
        SkipHttpErrorCheck = $true
    }
    if ($null -ne $Body) {
        $params.ContentType = 'application/json'
        $params.Body = ($Body | ConvertTo-Json -Compress -Depth 10)
    }
    $response = Invoke-WebRequest @params
    [pscustomobject]@{
        Status = [int]$response.StatusCode
        Json = ($response.Content | ConvertFrom-Json)
    }
}

function Assert-Equal {
    param($Actual, $Expected, [string]$Label)
    if ($Actual -ne $Expected) { throw "$Label expected '$Expected' but got '$Actual'" }
}
```

Expected: barrier 驱动的两个真实 transaction 都成功且 revision 从 `N` 精确变为 `N+2`；顺序签发的 `ticket@N` 与 `ticket@N+1` 分别被下一次提交判为 `StalePolicy`。这是行锁证据，MockDatabase 结果不能替代。

- [ ] **Step 6: 验证 bootstrap、后续注册和零房间**

```powershell
$adminLogin = Invoke-JsonApi POST '/api/account/login' @{ username = 'admin'; password = 'test123456' }
Assert-Equal $adminLogin.Status 200 'admin login status'
$adminToken = $adminLogin.Json.data.token

$viewerCreate = Invoke-JsonApi POST '/api/account/create' @{ username = 'viewer1'; password = 'viewer123' }
Assert-Equal $viewerCreate.Status 200 'viewer register status'
$viewerToken = $viewerCreate.Json.data.token

$users = Invoke-JsonApi GET '/api/admin/users' $null $adminToken
$admin = $users.Json.data | Where-Object username -eq 'admin'
$viewer = $users.Json.data | Where-Object username -eq 'viewer1'
Assert-Equal $admin.role 'super_admin' 'bootstrap role'
Assert-Equal $admin.room_count 1 'bootstrap room count'
Assert-Equal $viewer.role 'user' 'public registration role'
Assert-Equal $viewer.room_count 0 'viewer room count'
```

Expected: seed 的首账号兼容成功；注册响应直接给 token；viewer 为 enabled user 且零房间。

- [ ] **Step 7: 验证四象限隐私矩阵**

```powershell
$room = (Invoke-JsonApi GET '/api/admin/rooms' $null $adminToken).Json.data | Select-Object -First 1
$streamPath = [Uri]::EscapeDataString([string]$room.stream_id)

function Set-RoomPrivacy {
    param([bool]$RequireLogin, [bool]$PasswordEnabled, [AllowNull()][string]$Password)
    $response = Invoke-JsonApi PUT "/api/admin/rooms/$($room.id)" @{
        require_login = $RequireLogin
        password_enabled = $PasswordEnabled
        password = $Password
    } $adminToken
    Assert-Equal $response.Status 200 'privacy update status'
    $response
}

function Request-RoomAccess {
    param([string]$Token = '', [AllowNull()][string]$Password = $null)
    Invoke-JsonApi POST "/api/live/rooms/$streamPath/access" @{
        guest_id = $guestId
        password = $Password
    } $Token
}

Set-RoomPrivacy $false $false $null | Out-Null
Assert-Equal (Request-RoomAccess).Status 200 'public guest'
Assert-Equal (Request-RoomAccess $viewerToken).Status 200 'public account'

Set-RoomPrivacy $true $false $null | Out-Null
Assert-Equal (Request-RoomAccess).Status 401 'login-only guest'
Assert-Equal (Request-RoomAccess $viewerToken).Status 200 'login-only account'

Set-RoomPrivacy $false $true $roomPassword | Out-Null
Assert-Equal (Request-RoomAccess).Status 403 'password-only guest missing password'
Assert-Equal (Request-RoomAccess '' $roomPassword).Status 200 'password-only guest correct password'
Assert-Equal (Request-RoomAccess $viewerToken).Status 403 'password-only account missing password'
Assert-Equal (Request-RoomAccess $viewerToken $roomPassword).Status 200 'password-only account correct password'
$hashConfigured = docker compose -f docker-compose.test.yml exec -T postgres psql -U yantube -d yantube -tAc "SELECT password_hash LIKE 'sha256`$32`$100000`$%' FROM live_room WHERE id = $($room.id);"
Assert-Equal $hashConfigured.Trim() 't' 'stored PBKDF2 room password'

Set-RoomPrivacy $true $true $null | Out-Null
Assert-Equal (Request-RoomAccess '' $roomPassword).Status 401 'login-and-password guest'
Assert-Equal (Request-RoomAccess $viewerToken 'wrong-password').Status 403 'login-and-password wrong password'
Assert-Equal (Request-RoomAccess $viewerToken $roomPassword).Status 200 'login-and-password account'

$adminRoomsJson = ((Invoke-JsonApi GET '/api/admin/rooms' $null $adminToken).Json | ConvertTo-Json -Depth 20)
if ($adminRoomsJson -match 'password_hash') { throw 'admin response exposed password_hash' }

Set-RoomPrivacy $false $false $null | Out-Null
$hashCleared = docker compose -f docker-compose.test.yml exec -T postgres psql -U yantube -d yantube -tAc "SELECT length(password_hash) = 0 FROM live_room WHERE id = $($room.id);"
Assert-Equal $hashCleared.Trim() 't' 'disabled password hash cleared'

$revisionBefore = [int](docker compose -f docker-compose.test.yml exec -T postgres psql -U yantube -d yantube -tAc "SELECT access_revision FROM live_room WHERE id = $($room.id);").Trim()
$ticketBeforeConcurrent = (Request-RoomAccess $viewerToken).Json.data.ticket
$roomId = [int]$room.id
$parallelUpdates = @(
    [pscustomobject]@{ require_login = $true; password_enabled = $false; password = $null },
    [pscustomobject]@{ require_login = $true; password_enabled = $true; password = 'concurrent-pass' }
)
$parallelResponses = @($parallelUpdates | ForEach-Object -ThrottleLimit 2 -Parallel {
    $json = $_ | ConvertTo-Json -Compress
    $targetUri = '{0}/api/admin/rooms/{1}' -f $using:base, $using:roomId
    Invoke-WebRequest -Method PUT -Uri $targetUri -Headers @{ Authorization = "Bearer $using:adminToken" } -ContentType 'application/json' -Body $json -SkipHttpErrorCheck
})
Assert-Equal $parallelResponses.Count 2 'parallel privacy response count'
foreach ($response in $parallelResponses) {
    Assert-Equal ([int]$response.StatusCode) 200 'parallel privacy status'
}
$revisionAfter = [int](docker compose -f docker-compose.test.yml exec -T postgres psql -U yantube -d yantube -tAc "SELECT access_revision FROM live_room WHERE id = $($room.id);").Trim()
Assert-Equal $revisionAfter ($revisionBefore + 2) 'concurrent revision increments'

$preConcurrentCallback = Invoke-JsonApi POST "/api/internal/srs/on_play`?callback_secret=$callbackSecret" @{
    action = 'on_play'; app = 'live'; stream = $room.stream_id
    client_id = 'pre-concurrent-ticket'; param = "?ticket=$ticketBeforeConcurrent"
}
Assert-Equal $preConcurrentCallback.Json.code 1 'pre-concurrent ticket is stale'

$ticketAfterConcurrent = (Request-RoomAccess $viewerToken 'concurrent-pass').Json.data.ticket
Set-RoomPrivacy $false $false $null | Out-Null
$postConcurrentCallback = Invoke-JsonApi POST "/api/internal/srs/on_play`?callback_secret=$callbackSecret" @{
    action = 'on_play'; app = 'live'; stream = $room.stream_id
    client_id = 'post-concurrent-ticket'; param = "?ticket=$ticketAfterConcurrent"
}
Assert-Equal $postConcurrentCallback.Json.code 1 'post-concurrent ticket is stale after next setting'
```

Expected: 四个组合的 guest/account 结果与设计矩阵完全一致；已配置密码只以 PBKDF2 hash 存储，关闭开关会清空 hash，所有响应都不含 hash。两个 `ForEach-Object -Parallel` HTTP 更新都成功且 revision 精确 `N+2`；并发前 ticket 和并发后新签 ticket 都在下一次实际设置后失效。Task 6 Step 5 同时证明每次连续 commit 都会使前一 ticket stale；这里的并发请求不得改成循环顺序调用。

- [ ] **Step 8: 验证 ticket admission、revision 和唯一计数**

```powershell
function Invoke-SrsCallback {
    param([string]$Name, [hashtable]$Body)
    Invoke-JsonApi POST "/api/internal/srs/$Name`?callback_secret=$callbackSecret" $Body
}

function Assert-CallbackCode {
    param([object]$Response, [int]$Code, [string]$Label)
    Assert-Equal $Response.Status 200 "$Label HTTP status"
    Assert-Equal $Response.Json.code $Code "$Label callback code"
}

function Assert-ViewerCount {
    param([int]$Expected, [string]$Label)
    $metadata = Invoke-JsonApi GET "/api/live/rooms/$streamPath"
    Assert-Equal $metadata.Status 200 "$Label metadata status"
    Assert-Equal $metadata.Json.data.viewer_count $Expected "$Label viewer count"
}

Set-RoomPrivacy $false $false $null | Out-Null
$guestTicket = (Request-RoomAccess).Json.data.ticket
$accountTicket = (Request-RoomAccess $viewerToken).Json.data.ticket

$guestPlay1 = @{ action = 'on_play'; app = 'live'; stream = $room.stream_id; client_id = 'g1'; param = "?ticket=$guestTicket" }
$guestPlay2 = @{ action = 'on_play'; app = 'live'; stream = $room.stream_id; client_id = 'g2'; param = "?ticket=$guestTicket" }
$accountPlay = @{ action = 'on_play'; app = 'live'; stream = $room.stream_id; client_id = 'a1'; param = "?ticket=$accountTicket" }

Assert-CallbackCode (Invoke-SrsCallback 'on_play' $guestPlay1) 0 'guest g1 play'
Assert-ViewerCount 1 'guest g1 play'
Assert-CallbackCode (Invoke-SrsCallback 'on_play' $guestPlay1) 0 'duplicate g1 play'
Assert-ViewerCount 1 'duplicate g1 play'
Assert-CallbackCode (Invoke-SrsCallback 'on_play' $guestPlay2) 0 'same guest g2 play'
Assert-ViewerCount 1 'same guest g2 play'
Assert-CallbackCode (Invoke-SrsCallback 'on_play' $accountPlay) 0 'account a1 play'
Assert-ViewerCount 2 'account a1 play'

Assert-CallbackCode (Invoke-SrsCallback 'on_stop' @{ action = 'on_stop'; stream = $room.stream_id; client_id = 'g1' }) 0 'guest g1 stop'
Assert-ViewerCount 2 'guest g1 stop'
Assert-CallbackCode (Invoke-SrsCallback 'on_stop' @{ action = 'on_stop'; stream = $room.stream_id; client_id = 'g2' }) 0 'guest g2 stop'
Assert-ViewerCount 1 'guest g2 stop'
Assert-CallbackCode (Invoke-SrsCallback 'on_stop' @{ action = 'on_stop'; stream = $room.stream_id; client_id = 'g2' }) 0 'duplicate g2 stop'
Assert-ViewerCount 1 'duplicate g2 stop'
Assert-CallbackCode (Invoke-SrsCallback 'on_stop' @{ action = 'on_stop'; stream = $room.stream_id; client_id = 'a1' }) 0 'account a1 stop'
Assert-ViewerCount 0 'account a1 stop'

Assert-CallbackCode (Invoke-SrsCallback 'on_play' @{ action = 'on_play'; stream = $room.stream_id; client_id = 'missing-ticket'; param = '' }) 1 'missing ticket'

$adminMe = (Invoke-JsonApi GET '/api/admin/me' $null $adminToken).Json.data
$allRooms = (Invoke-JsonApi GET '/api/admin/rooms' $null $adminToken).Json.data
$crossRoom = $allRooms | Where-Object stream_id -eq 'ticket-cross-room' | Select-Object -First 1
if ($null -eq $crossRoom) {
    $crossRoom = (Invoke-JsonApi POST '/api/admin/rooms' @{
        user_id = $adminMe.id
        stream_id = 'ticket-cross-room'
        title = 'Ticket Cross Room'
        enabled = $true
        require_login = $false
        password_enabled = $false
        password = $null
    } $adminToken).Json.data
}
Assert-CallbackCode (Invoke-SrsCallback 'on_play' @{ action = 'on_play'; stream = $crossRoom.stream_id; client_id = 'cross-room'; param = "?ticket=$guestTicket" }) 1 'cross-room ticket'

$staleTicket = (Request-RoomAccess).Json.data.ticket
Set-RoomPrivacy $true $false $null | Out-Null
Assert-CallbackCode (Invoke-SrsCallback 'on_play' @{ action = 'on_play'; stream = $room.stream_id; client_id = 'stale-ticket'; param = "?ticket=$staleTicket" }) 1 'stale ticket'
Set-RoomPrivacy $false $false $null | Out-Null

$freshGuestTicket = (Request-RoomAccess).Json.data.ticket
$freshAccountTicket = (Request-RoomAccess $viewerToken).Json.data.ticket
Assert-CallbackCode (Invoke-SrsCallback 'on_play' @{ action = 'on_play'; stream = $room.stream_id; client_id = 'clear-g'; param = "?ticket=$freshGuestTicket" }) 0 'pre-unpublish guest'
Assert-CallbackCode (Invoke-SrsCallback 'on_play' @{ action = 'on_play'; stream = $room.stream_id; client_id = 'clear-a'; param = "?ticket=$freshAccountTicket" }) 0 'pre-unpublish account'
Assert-ViewerCount 2 'before unpublish'
Assert-CallbackCode (Invoke-SrsCallback 'on_unpublish' @{ action = 'on_unpublish'; stream = $room.stream_id; client_id = 'publisher-1' }) 0 'unpublish clear'
Assert-ViewerCount 0 'after unpublish'

$restartTicket = (Request-RoomAccess).Json.data.ticket
Assert-CallbackCode (Invoke-SrsCallback 'on_play' @{ action = 'on_play'; stream = $room.stream_id; client_id = 'restart-check'; param = "?ticket=$restartTicket" }) 0 'pre-restart play'
Assert-ViewerCount 1 'before API restart'
docker compose -f docker-compose.test.yml restart api
if ($LASTEXITCODE -ne 0) { throw "api restart during presence test failed with exit code $LASTEXITCODE" }
Wait-YantubeApi
Assert-ViewerCount 0 'after API restart'
```

Expected: callback code、身份去重、client 幂等、最后会话移除、跨房间、stale revision、unpublish 清零和单实例 API restart 归零全部通过；过期边界由 Task 2 的注入时钟单元测试证明。

- [ ] **Step 9: 用 .NET ClientWebSocket 验证实时弹幕**

```powershell
function Connect-RoomSocket {
    param([string]$Ticket)
    $socket = [System.Net.WebSockets.ClientWebSocket]::new()
    $encodedTicket = [Uri]::EscapeDataString($Ticket)
    $uri = [Uri]"ws://127.0.0.1:5174/api/live/rooms/$streamPath/ws?ticket=$encodedTicket"
    $socket.ConnectAsync($uri, [Threading.CancellationToken]::None).GetAwaiter().GetResult()
    [void]$qaSockets.Add($socket)
    $socket
}

function Send-RoomText {
    param([System.Net.WebSockets.ClientWebSocket]$Socket, [string]$Text)
    $bytes = [Text.Encoding]::UTF8.GetBytes($Text)
    $segment = [ArraySegment[byte]]::new($bytes)
    $Socket.SendAsync($segment, [System.Net.WebSockets.WebSocketMessageType]::Text, $true, [Threading.CancellationToken]::None).GetAwaiter().GetResult()
}

function Receive-RoomJson {
    param([System.Net.WebSockets.ClientWebSocket]$Socket, [int]$TimeoutMs = 5000)
    $buffer = [byte[]]::new(4096)
    $segment = [ArraySegment[byte]]::new($buffer)
    $memory = [IO.MemoryStream]::new()
    $cts = [Threading.CancellationTokenSource]::new($TimeoutMs)
    try {
        do {
            $result = $Socket.ReceiveAsync($segment, $cts.Token).GetAwaiter().GetResult()
            if ($result.MessageType -eq [System.Net.WebSockets.WebSocketMessageType]::Close) {
                return [pscustomobject]@{ type = 'close'; code = [int]$Socket.CloseStatus; reason = $Socket.CloseStatusDescription }
            }
            $memory.Write($buffer, 0, $result.Count)
        } while (-not $result.EndOfMessage)
        $text = [Text.Encoding]::UTF8.GetString($memory.ToArray())
        $text | ConvertFrom-Json
    } finally {
        $cts.Dispose()
        $memory.Dispose()
    }
}

$wsGuestTicket = (Request-RoomAccess).Json.data.ticket
$wsAccountTicket = (Request-RoomAccess $viewerToken).Json.data.ticket
$guestSocket = Connect-RoomSocket $wsGuestTicket
$accountSocket = Connect-RoomSocket $wsAccountTicket
Assert-Equal (Receive-RoomJson $guestSocket).type 'viewer_count' 'guest initial count event'
Assert-Equal (Receive-RoomJson $accountSocket).type 'viewer_count' 'account initial count event'

Send-RoomText $guestSocket '{"type":"send_message","content":"<b>literal</b>"}'
Send-RoomText $guestSocket '{"type":"send_message","content":"too-fast"}'
$guestEvents = @(
    Receive-RoomJson $guestSocket
    Receive-RoomJson $guestSocket
)
$guestAccepted = @($guestEvents | Where-Object {
    $_.type -eq 'danmaku' -and $_.content -eq '<b>literal</b>'
})
$guestRateLimited = @($guestEvents | Where-Object {
    $_.type -eq 'error' -and $_.code -eq 'rate_limited'
})
Assert-Equal $guestAccepted.Count 1 'guest accepted danmaku event count'
Assert-Equal $guestRateLimited.Count 1 'guest rate-limited event count'
$guestEcho = $guestAccepted[0]
$accountGuestMessage = Receive-RoomJson $accountSocket
Assert-Equal $guestEcho.type 'danmaku' 'guest echo type'
Assert-Equal $guestEcho.sender.kind 'guest' 'guest echo sender kind'
Assert-Equal $guestEcho.content '<b>literal</b>' 'guest echo plain text'
Assert-Equal $accountGuestMessage.type 'danmaku' 'account receives only accepted event'
Assert-Equal $accountGuestMessage.sender.kind 'guest' 'guest sender kind'
Assert-Equal $accountGuestMessage.sender.name '游客-550E' 'guest sender name'
Assert-Equal $accountGuestMessage.content '<b>literal</b>' 'plain text preservation'

Start-Sleep -Milliseconds 1100
$tooLong = '界' * 101
Send-RoomText $guestSocket (@{ type = 'send_message'; content = $tooLong } | ConvertTo-Json -Compress)
$invalidMessage = Receive-RoomJson $guestSocket
Assert-Equal $invalidMessage.code 'invalid_message' 'overlong validation'
try {
    $unexpected = Receive-RoomJson $accountSocket 750
    throw "overlong message was broadcast as $($unexpected.type)"
} catch [System.OperationCanceledException] {
}

Send-RoomText $accountSocket '{"type":"send_message","content":"account-message"}'
$accountEcho = Receive-RoomJson $accountSocket
$guestAccountMessage = Receive-RoomJson $guestSocket
Assert-Equal $accountEcho.sender.kind 'user' 'account echo kind'
Assert-Equal $guestAccountMessage.sender.name 'viewer1' 'account sender name'

$newSocket = Connect-RoomSocket ((Request-RoomAccess).Json.data.ticket)
Assert-Equal (Receive-RoomJson $newSocket).type 'viewer_count' 'new socket initial event'
try {
    $replayed = Receive-RoomJson $newSocket 750
    throw "danmaku history replayed as $($replayed.type)"
} catch [System.OperationCanceledException] {
}

$invalidSocket = Connect-RoomSocket 'invalid-room-ticket'
$closeEvent = Receive-RoomJson $invalidSocket
Assert-Equal $closeEvent.type 'close' 'invalid ticket close event'
Assert-Equal $closeEvent.code 1008 'invalid ticket close code'

foreach ($socket in @($guestSocket, $accountSocket, $newSocket, $invalidSocket)) { $socket.Dispose() }
```

Expected: 初始 count、guest/account sender、plain text、无历史回放、限流、长度校验和 1008 close 全部通过；guest 的两个返回事件按集合分类，顺序任意但必须恰有一个 accepted danmaku 和一个 `rate_limited` error，account 只收到 accepted danmaku。WS 连接本身不改变 metadata viewer count。

- [ ] **Step 10: 真实 RTMP、HLS `hls_ctx` 链路与 WHEP 零基线**

先只检查已有 ffmpeg，不运行安装。缺少时把 RTMP/HLS/WHEP 媒体项记录为未验证并继续非媒体 QA，最终 Step 13 以未完成证据失败，而不是静默跳过：

```powershell
$ffmpeg = Get-Command ffmpeg -ErrorAction SilentlyContinue
if ($null -eq $ffmpeg) {
    [void]$qaUnverified.Add('RTMP publish + HLS/WHEP playback: existing ffmpeg executable unavailable')
} else {
    $rooms = Invoke-JsonApi GET '/api/admin/rooms' $null $adminToken
    $room = $rooms.Json.data | Select-Object -First 1
    $publishUrl = "rtmp://127.0.0.1:1935/live/$($room.stream_id)?token=$([Uri]::EscapeDataString($room.stream_code))"
    $ffmpegArgs = @('-hide_banner','-loglevel','warning','-re','-f','lavfi','-i','testsrc=size=1280x720:rate=30','-f','lavfi','-i','sine=frequency=1000:sample_rate=48000','-c:v','libx264','-preset','veryfast','-tune','zerolatency','-c:a','aac','-f','flv',$publishUrl)
    $publisher = Start-Process -FilePath $ffmpeg.Source -ArgumentList $ffmpegArgs -PassThru
    Start-Sleep -Seconds 8
    if ($publisher.HasExited) { throw "ffmpeg publisher exited early with code $($publisher.ExitCode)" }

    Set-RoomPrivacy $false $false $null | Out-Null
    $hlsTicket = (Request-RoomAccess).Json.data.ticket
    $encodedHlsTicket = [Uri]::EscapeDataString($hlsTicket)
    $hlsUrl = "http://127.0.0.1:5174/live/$($room.stream_id).m3u8?ticket=$encodedHlsTicket"
    $playlistFile = Join-Path $env:TEMP 'yantube-valid.m3u8'
    $childPlaylistFile = Join-Path $env:TEMP 'yantube-valid-child.m3u8'
    $segmentFile = Join-Path $env:TEMP 'yantube-valid.ts'
    $missingPlaylistFile = Join-Path $env:TEMP 'yantube-missing.m3u8'
    $stalePlaylistFile = Join-Path $env:TEMP 'yantube-stale.m3u8'
    foreach ($path in @($playlistFile, $childPlaylistFile, $segmentFile, $missingPlaylistFile, $stalePlaylistFile)) {
        [void]$qaTempFiles.Add($path)
        if (Test-Path -LiteralPath $path) { Remove-Item -LiteralPath $path -Force }
    }

    function Invoke-CurlProbe {
        param([string]$Uri, [string]$OutputPath)
        $probe = & curl.exe --silent --show-error --max-time 10 --output $OutputPath --write-out '%{http_code}|%{content_type}' $Uri
        if ($LASTEXITCODE -ne 0) { throw "curl probe failed for media endpoint with exit code $LASTEXITCODE" }
        $parts = [string]$probe -split '\|', 2
        [pscustomobject]@{ Status = [int]$parts[0]; ContentType = [string]$parts[1]; Path = $OutputPath }
    }

    function Test-ValidHlsPlaylist {
        param([object]$Probe)
        if (-not (Test-Path -LiteralPath $Probe.Path)) { return $false }
        $text = Get-Content -LiteralPath $Probe.Path -Raw
        $Probe.Status -eq 200 -and
            $Probe.ContentType -match '(?i)(application|audio)/(vnd\.apple\.|x-)?mpegurl' -and
            $text.TrimStart().StartsWith('#EXTM3U')
    }

    function Get-HlsUriLines {
        param([string]$Path)
        @(
            (Get-Content -LiteralPath $Path -Raw) -split "`r?`n" |
                ForEach-Object { $_.Trim() } |
                Where-Object { $_ -and -not $_.StartsWith('#') }
        )
    }

    $validPlaylistProbe = $null
    for ($attempt = 1; $attempt -le 20; $attempt++) {
        $validPlaylistProbe = Invoke-CurlProbe $hlsUrl $playlistFile
        if (Test-ValidHlsPlaylist $validPlaylistProbe) { break }
        Start-Sleep -Milliseconds 500
    }
    if (-not (Test-ValidHlsPlaylist $validPlaylistProbe)) {
        throw "valid-ticket HLS was not a 200 mpegurl #EXTM3U playlist"
    }

    $mediaPlaylistUri = [Uri]$hlsUrl
    $mediaPlaylistFile = $playlistFile
    $initialUris = Get-HlsUriLines $playlistFile
    $segmentLine = $initialUris |
        Where-Object { $_ -match '\.ts(?:\?|$)' } |
        Select-Object -First 1

    if (-not $segmentLine) {
        $childLine = $initialUris |
            Where-Object { $_ -match '\.m3u8(?:\?|$)' } |
            Select-Object -First 1
        if (-not $childLine) { throw 'valid HLS master exposed neither child m3u8 nor media segment' }

        # Follow the exact SRS-emitted absolute/relative child URI. Uri resolution preserves
        # its hls_ctx/query; do not append the initial ticket when SRS omitted it.
        $childUri = [Uri]::new([Uri]$hlsUrl, [string]$childLine)
        $childProbe = $null
        for ($childAttempt = 1; $childAttempt -le 20; $childAttempt++) {
            $childProbe = Invoke-CurlProbe $childUri.AbsoluteUri $childPlaylistFile
            if (Test-ValidHlsPlaylist $childProbe) { break }
            Start-Sleep -Milliseconds 250
        }
        if (-not (Test-ValidHlsPlaylist $childProbe)) {
            throw 'HLS child was not a 200 mpegurl #EXTM3U media playlist'
        }
        $mediaPlaylistUri = $childUri
        $mediaPlaylistFile = $childPlaylistFile
        $segmentLine = Get-HlsUriLines $mediaPlaylistFile |
            Where-Object { $_ -match '\.ts(?:\?|$)' } |
            Select-Object -First 1
    }

    if (-not $segmentLine) { throw 'valid HLS media playlist did not expose a readable .ts URI' }
    # Follow the exact media-playlist URI. Absolute/relative resolution retains any query
    # emitted on the .ts line; never synthesize, replace, or drop hls_ctx/ticket parameters.
    $segmentUri = [Uri]::new($mediaPlaylistUri, [string]$segmentLine)
    $segmentProbe = Invoke-CurlProbe $segmentUri.AbsoluteUri $segmentFile
    Assert-Equal $segmentProbe.Status 200 'HLS segment HTTP status'
    if ($segmentProbe.ContentType -notmatch '(?i)(video/mp2t|application/octet-stream)') {
        throw "HLS segment content type was '$($segmentProbe.ContentType)'"
    }
    if ((Get-Item -LiteralPath $segmentFile).Length -le 0) { throw 'HLS segment was not readable' }

    $missingProbe = Invoke-CurlProbe "http://127.0.0.1:5174/live/$($room.stream_id).m3u8" $missingPlaylistFile
    if (Test-ValidHlsPlaylist $missingProbe) { throw 'missing-ticket HLS returned a valid playlist' }
    if ($missingProbe.Status -eq 200) {
        $missingText = Get-Content -LiteralPath $missingPlaylistFile -Raw
        if ($missingText.TrimStart().StartsWith('#EXTM3U')) { throw 'missing-ticket HLS returned #EXTM3U' }
    }

    $staleHlsTicket = (Request-RoomAccess).Json.data.ticket
    Set-RoomPrivacy $true $false $null | Out-Null
    Set-RoomPrivacy $false $false $null | Out-Null
    $staleHlsUrl = "http://127.0.0.1:5174/live/$($room.stream_id).m3u8?ticket=$([Uri]::EscapeDataString($staleHlsTicket))"
    $staleProbe = Invoke-CurlProbe $staleHlsUrl $stalePlaylistFile
    if (Test-ValidHlsPlaylist $staleProbe) { throw 'stale-ticket HLS returned a valid playlist' }
    if ($staleProbe.Status -eq 200) {
        $staleText = Get-Content -LiteralPath $stalePlaylistFile -Raw
        if ($staleText.TrimStart().StartsWith('#EXTM3U')) { throw 'stale-ticket HLS returned #EXTM3U' }
    }

    # SRS 6 hls_ctx may keep a fake playback session for about 2*hls_window. Reset the
    # in-memory API hub instead of sleeping 40 seconds before WHEP identity-count QA.
    docker compose -f docker-compose.test.yml restart api
    if ($LASTEXITCODE -ne 0) { throw "api restart for post-HLS baseline failed with exit code $LASTEXITCODE" }
    Wait-YantubeApi
    Assert-ViewerCount 0 'post-HLS API restart baseline'
    $postHlsGuestTicket = (Request-RoomAccess).Json.data.ticket
    $postHlsAccountTicket = (Request-RoomAccess $viewerToken).Json.data.ticket
}
```

Expected: ffmpeg 存在时真实 RTMP publish 保持运行；有效初始 HLS ticket 得到 HTTP 200、mpegurl Content-Type、`#EXTM3U`。若它是 SRS 6 `hls_ctx` master，则严格跟随返回的 child URI（包括其 `hls_ctx` query）再次验证 media playlist，再严格跟随 media 返回的 `.ts` URI读取分片；不得给 child/segment 擅自追加 ticket 或重建 query。missing/stale **初始** playlist 必须得到拒绝状态，或至少不能形成 200 + mpegurl + `#EXTM3U` 的有效组合，禁止以文件字节数单独判断拒绝。HLS 完成后不等待约 40 秒，而是重启 API、等待 ready、断言 count=0 并取得 fresh tickets，WHEP 不得复用 HLS 前的 ticket/presence。`deploy/srs/srs.conf` 无 `http_remux`，所以不请求 `.flv`。

- [ ] **Step 11: 用现有 Playwright/CDP 真实播放 WHEP，并验收 UI/stale metadata**

只使用执行代理已经具备的 Browser/CDP/Playwright 能力；禁止运行 `pnpm add`、`pnpm exec playwright install` 或下载浏览器。若能力不存在，在同一 PowerShell 会话执行 `[void]$qaUnverified.Add('WHEP/UI real-surface QA: existing Playwright/CDP capability unavailable')`，继续日志和清理，并在 Step 13 报告未验证；不得用 URL 字符串检查冒充播放。

ffmpeg publisher 正常时，浏览器测试使用 `try/finally`，在 `finally` 中关闭所有 page/context；截图只写 `$env:TEMP`。先再次断言 Step 10 API restart 后 metadata count 为 0，把房间设为 public，并丢弃所有 restart 前 ticket。每个 page 必须观察到 restart 后的新 `POST .../access` 并使用其 ticket，不能注入 HLS 前的 ticket，然后执行以下真实 WHEP 场景：

1. 创建三个**独立 BrowserContext**：`guestA`、`guestB`、`account`。在两个 guest context 的 `addInitScript` 中都执行 `localStorage.setItem('yantube_guest_id', '550e8400-e29b-41d4-a716-446655440000')`；这就是跨 context 共享身份，不能只在同一 context 开两个 page。account context 设置 `localStorage.setItem('jwt', viewerToken)`，guest ID 可为另一个 UUID。
2. 对每个 page 在 request listener 中收集 `/rtc/v1/whep/` 请求；打开 `/live/:stream_id`，等待协议按钮出现，点击当前协议按钮并选择 `menuitem` 文本 `webrtc`。等待 `video.readyState >= 2`，并断言 `video.srcObject` 存在且至少一个 video track 的 `readyState === 'live'`；只有 URL 出现不算播放成功。
3. 对每个 WHEP request 解析 URL，断言 `searchParams.get('ticket')` 非空，且 `request.headers()` 不含 `authorization`；账号 JWT 不得出现在 URL。HLS 的 status/Content-Type/playlist/segment 已由 Step 10 验收；FLV 只由单元测试验收。
4. guestA 与 guestB 都播放后轮询 metadata，人数必须为 1；account 开始播放后必须为 2。关闭 guestA context 后仍为 2；关闭 guestB 后为 1；关闭 account 后为 0。每次最多轮询 10 秒，每 250ms 一次；断言的是 SRS `on_play/on_stop` 驱动的 metadata 值，不读取 SRS `clients`，WebSocket 数量也不参与。

同一 browser runner 继续执行三类 stale metadata race；每类用新 context/page，记录该恢复周期内 `GET /api/live/rooms/:stream_id` 与 `POST .../access` 请求数：

1. **public→login：** 先设置 public；用 one-shot `page.route('**/access')` 暂停首次 access（此前 metadata 已返回 public），handler 捕获该请求后立即 `page.unroute`，管理员 API 改为 `require_login=true` 后再 `route.continue()`。首次 POST 返回 401；断言随后恰好强制 GET metadata 1 次、自动 POST 0 次、显示 `room-access-gate` 登录门且没有 video。
2. **public→password：** 同样用 one-shot route 暂停 public 的首次 access，管理员改为 `password_enabled=true,password='new-room-pass'` 后继续。首次 POST 返回 403；断言 refresh 恰好 1 次、自动 POST 0 次、显示 password gate 且没有 video。
3. **password change：** 初始设置旧密码 `old-room-pass` 并打开 password gate；管理员改成 `new-room-pass`，用户提交旧密码得到 403。断言随后 refresh 1 次、没有自动第二次 access、password input 被清空并停在 gate；等待 16 秒确认没有 timer 重试。用户输入新密码并点击提交，这是新恢复周期，恰好一次 access 后进入 ready。

最后在 375x812、768x1024、1280x900 分别检查：

```text
document.documentElement.scrollWidth <= window.innerWidth
首页 viewer-count 与两个 privacy chip 可见，私密房仍可进入 gate
登录/注册切换和安全 redirect 返回房间
danmaku-composer、danmaku-recent、danmaku-overlay 存在
overlay computed pointer-events = none 且 bounds 不覆盖 controls bounds
prefers-reduced-motion 下动画 duration 为 0，静态消息约 6 秒后移除
```

Expected: 三个独立真实 WHEP 会话进入可播放状态；共享 guest 的两会话只计 1，账号使总数为 2，关闭语义为 `2→2→1→0`；所有 WHEP 请求有 room ticket 且无 Authorization。三种策略转换都以最新 metadata 选 gate，每个恢复周期至多一次 refresh + 一次 access，耗尽后必须等用户动作。

- [ ] **Step 12: 验证 nginx 和日志脱敏**

```powershell
docker compose -f docker-compose.test.yml exec -T front nginx -t
if ($LASTEXITCODE -ne 0) { throw "nginx config test failed with exit code $LASTEXITCODE" }
$appLogs = docker compose -f docker-compose.test.yml logs --no-color api front
if (-not ($appLogs -match [regex]::Escape('/api/internal/srs/heartbeat/:callback_secret'))) {
    throw 'matched heartbeat route template was not observed in API logs'
}
$secretsToScan = @(
    $callbackSecret, $adminToken, $viewerToken, $roomPassword,
    'concurrent-pass', 'old-room-pass', 'new-room-pass', $room.stream_code,
    $ticketBeforeConcurrent, $ticketAfterConcurrent,
    $guestTicket, $accountTicket, $staleTicket,
    $freshGuestTicket, $freshAccountTicket, $restartTicket,
    $wsGuestTicket, $wsAccountTicket,
    $hlsTicket, $staleHlsTicket, $postHlsGuestTicket, $postHlsAccountTicket
) |
    Where-Object { -not [string]::IsNullOrEmpty([string]$_) }
foreach ($secret in $secretsToScan) {
    if ($appLogs -match [regex]::Escape($secret)) { throw 'credential leaked into api/front logs' }
}
if ($appLogs -match 'sha256\$32\$100000\$') { throw 'password hash leaked into api/front logs' }
```

Expected: nginx 配置通过；WS 经 5174 入口可连接；API/front logs 不包含 callback secret、ticket、JWT、推流码、明文密码或 hash。真实 heartbeat 即使把 secret 放在 path 中，API span 也只能显示 `/api/internal/srs/heartbeat/:callback_secret`；SRS 第三方自身日志单独保存为诊断证据，不把它的 `clients` 值当人数。

- [ ] **Step 13: 用 finally 无条件清理，并显式报告未验证项**

```powershell
} finally {
    foreach ($socket in $qaSockets) {
        try { $socket.Dispose() } catch { [void]$qaCleanupFailures.Add("socket dispose: $($_.Exception.Message)") }
    }
    if ($publisher -and -not $publisher.HasExited) {
        try { Stop-Process -Id $publisher.Id -Force -ErrorAction Stop } catch { [void]$qaCleanupFailures.Add("ffmpeg stop: $($_.Exception.Message)") }
    }
    foreach ($path in $qaTempFiles) {
        if (Test-Path -LiteralPath $path) {
            try { Remove-Item -LiteralPath $path -Force -ErrorAction Stop } catch { [void]$qaCleanupFailures.Add("temp cleanup $path`: $($_.Exception.Message)") }
        }
    }
    Remove-Item Env:YANTUBE_TEST_DATABASE_URL -ErrorAction SilentlyContinue
    if ($composeAttempted) {
        docker compose -f docker-compose.test.yml down
        if ($LASTEXITCODE -ne 0) { [void]$qaCleanupFailures.Add("compose down exit code $LASTEXITCODE") }
    }
}

if ($qaCleanupFailures.Count -gt 0) {
    throw "QA cleanup failed: $($qaCleanupFailures -join '; ')"
}
if ($qaUnverified.Count -gt 0) {
    throw "QA incomplete; report as UNVERIFIED and do not install tools: $($qaUnverified -join '; ')"
}
```

Expected: 无论 Step 4-12 在何处抛错，都 dispose 所有已登记 `.NET ClientWebSocket`、停止 ffmpeg、删除 HLS/temp media、移除测试 DB 环境变量并执行 Compose `down`；浏览器 runner 自身的 `finally` 同时关闭 pages/contexts。正常成功时 test containers 停止并保留 volume；缺少 ffmpeg 或现有 browser capability 时清理后明确以 `UNVERIFIED` 失败，绝不安装软件或伪报通过。

- [ ] **Step 14: 检查两个仓库工作树边界**

只读执行：

```powershell
git status --short
git diff --check
```

分别在 `api_rs/`、`front/` 运行。Expected: 只有文件职责地图列出的预期文件变化；`deploy/`、设计规格、`front/DESIGN.md` 无变化；`git diff --check` 退出码 0。

- [ ] **Step 15: 记录最终提交边界（BLOCKED pending explicit user permission）**

建议分别提交：`feat(api): add viewer presence chat and private rooms`、`feat(front): add private viewing and live danmaku`。不要执行任何 git 写命令。

---

## 规格覆盖表

| 规格要求 | 实现任务 | 自动/真实证据 |
|---|---|---|
| 首账号 bootstrap super_admin/default room；后续公开注册 user/零房间；注册返回 JWT | Task 4、10 | account tests；Task 15 Step 6 Compose seed/注册/admin users 断言 |
| migration 09、旧房间公开、幂等 | Task 1 | db tests；Task 15 Step 4 fresh DB/restart/SQL |
| 独立 require_login/password 双开关、6-64 Unicode、write-only、并发安全 revision | Task 2、6、14 | pure/permission tests；真实 PostgreSQL barrier test；Task 15 Step 7 四象限、并发 HTTP 与 `N→N+2` DB 断言 |
| 15 分钟、kind/签名/room/revision/attestation ticket | Task 2、5 | room_access tests；Task 15 Step 8 callback admission |
| 当前房间元数据与 live list flags/count，私密房仍可发现，RSS 不变 | Task 5、13 | live/live_feed tests；Compose UI/API |
| SRS on_play/on_stop/on_unpublish、client 幂等、identity 引用计数 | Task 3、7 | hub/callback tests；Task 15 Step 8 direct callbacks 与 Step 11 三个真实 WHEP 会话 |
| WS 不增加人数、初始/实时 viewer_count | Task 3、8、12 | hub tests；Task 15 Step 9/11 |
| guest/account 弹幕、plain text、1-100、1 秒限流、无历史、slow receiver 恢复 | Task 8、12 | danmaku tests；Task 15 Step 9 以无序集合分类 accepted/rate_limited 事件 |
| guest UUID localStorage、确定性游客名、账号可见状态、logout | Task 2、9、10 | Rust/TS pure tests；browser QA |
| 登录/注册安全 redirect、注册后直接登录 | Task 9、10 | redirect tests；browser QA |
| WHEP/HLS/FLV builder 全部含 ticket，不依赖 WHEP Bearer | Task 9、11 | streamUrls tests 覆盖三种 builder；Task 15 Step 10 跟随 SRS 6 `hls_ctx` master→media→segment 并 reset API baseline，Step 11 真实 WHEP 可播放、ticket query 与无 Authorization |
| Compose 未启用 `http_remux`，不伪报 FLV 真实表面 | Task 15 | 原样读取 `deploy/srs/srs.conf`；不请求 `.flv`，FLV 仅由 builder 单元测试证明 |
| ticket access/WS 失效后的 fresh metadata 与单周期恢复预算 | Task 12 | roomAccessState tests；Task 15 Step 11 public→login、public→password、password change 网络计数 |
| player overlay、recent accessible region、reduced motion、控制栏不被覆盖 | Task 11、12 | build/lint；Task 15 Step 11 |
| 房主/Admin 隐私 UI、共用锁定更新 helper 和既有权限边界 | Task 6、14 | backend permission tests；真实 PostgreSQL 行锁/rollback tests；Compose UI |
| nginx WS Upgrade、现有限流/代理保留 | Task 15 | `nginx -t`；经 5174 的 ClientWebSocket |
| heartbeat path/callback/JWT/ticket/password/stream_code 日志脱敏 | Task 5、7、15 | 仅 `MatchedPath` 模板 tracing、nginx format；Task 15 扫描 `$callbackSecret` 与全部凭证并要求观察 heartbeat route template |
| 单实例内存限制、API 重启人数归零 | Task 3 | hub 架构；Compose restart 后 metadata count 0 |
| 最终质量门禁、工具缺失报告、失败路径 cleanup | Task 15 | 精确 PowerShell 命令；外层 try/finally；socket/ffmpeg/temp/browser/Compose 清理；UNVERIFIED 失败边界 |

## 自审结果

- **规格覆盖：通过。** 上表逐项映射设计文档的账号、迁移、隐私、ticket、presence、danmaku、前端、nginx、安全、测试和清理要求。
- **占位符扫描：通过。** 所有任务都有精确文件、类型/函数/JSON/SQL 契约、命令和预期结果；没有未决实现标记或模糊错误处理要求。
- **类型/名称一致性：通过。** 全计划统一使用 `RoomTicketClaims`、`ViewerIdentity`、`LiveHub`、`RoomEvent`、`RoomPrivacyInput`、`RoomUpdateActor`、`LockedRoomUpdate`、`RoomPrivacyUpdateError`、`PublicRoomMetadata`、`RecoveryBudget`、`viewer_count`、`require_login`、`has_password`、`password_enabled`、`access_revision`。
- **并发/恢复一致性：通过。** owner/admin 隐私变化都在 `update_room_with_privacy_locked` 的同一 transaction 中 `lock_exclusive → re-read → prepare → ActiveModel::update(&txn) → commit`；真实 PostgreSQL barrier 与并发 HTTP 分别证明无丢失 revision。401/403/1008/reacquire failure 都先取 fresh metadata，每周期最多一次 refresh/access。
- **tracing/事件顺序检查：通过。** HTTP span 只取 `MatchedPath::as_str()`，未匹配固定为 `unmatched`，不读取 raw URI/path/query/header；heartbeat callback secret 纳入真实日志扫描。WS QA 对 guest 的两个事件按 `type/code` 分类，不假定 danmaku 与 rate-limit error 的到达顺序。
- **边界一致性：通过。** presence 只由 SRS callbacks 驱动；WS 只订阅；账号 JWT 只用于账号身份；room ticket 只用于媒体/WS；密码/hash 均不跨后端边界。Compose 真实媒体只覆盖当前配置启用的 HLS/WHEP；HLS 跟随 SRS 6 `hls_ctx` 输出，完成后以 API restart/reset 建立 WHEP count=0 baseline，不等待 40 秒。FLV builder 仍完整但不虚构 real-surface 证据。
- **仓库/权限检查：通过。** 计划只写 `api_rs/`、`front/`，`deploy/` 只验证；所有 commit 步骤均标记 `BLOCKED pending explicit user permission`。
- **可执行性检查：通过。** 所有 shell 片段为 PowerShell 7 语法，不含 Bash 环境变量写法、链式操作符或 POSIX 空设备路径；Compose 段由一个外层 try/finally 包围。浏览器/ffmpeg 只复用已有工具，缺失时清理后明确 `UNVERIFIED`，禁止安装或伪报通过。

## 执行顺序

严格按波次 0 → 6 执行；每个任务先 RED、再最小 GREEN、再局部回归。Task 15 是唯一全量与真实表面验收点；任何前序任务修改了已通过任务的接口，都必须先重跑受影响的局部门禁再进入下一波。
