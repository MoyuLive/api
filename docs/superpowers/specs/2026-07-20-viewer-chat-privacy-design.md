# Yantube Viewer Accounts, Danmaku, Viewer Count, and Private Rooms Design

## Scope and constraints

Implement four connected capabilities while modifying only `api_rs/` and `front/`:

1. Current unique viewers for each live room.
2. Viewer registration, login, logout, and visible account state.
3. Real-time danmaku for authenticated users and guests.
4. Per-room privacy controls for login requirement and room password, independently switchable.

The existing Rust API, PostgreSQL, SRS 6, React/MUI frontend, JWT accounts, roles, and room ownership model remain authoritative. Docker Compose under `deploy/` may be used unchanged for verification. No Redis, message queue, OAuth, password recovery, follows, gifts, moderation console, or stored chat history is added.

## Chosen architecture

Use a unified room-access ticket to connect privacy authorization, SRS playback, viewer counting, and WebSocket danmaku:

1. A browser asks the API for room metadata.
2. It requests a room-access ticket with its optional account JWT, stable guest ID, and optional room password.
3. The API evaluates both privacy switches and signs a 15-minute ticket bound to the room, viewer identity, and room access revision.
4. The browser appends the ticket to WHEP, HLS, or HTTP-FLV playback URLs and uses it for the room WebSocket.
5. SRS sends the query string to `on_play`; the API validates the ticket and records the playback session by SRS `client_id`.
6. `on_stop` removes that session. The in-memory room hub broadcasts the resulting unique-viewer count and danmaku events.

This is selected over SRS `clients` because SRS cannot express platform identity or multi-tab deduplication. PostgreSQL-backed presence and chat are rejected because the current deployment is a single API instance and no persistence requirement exists.

## Account model

- Reuse the existing `user` table, JWT format, and roles. A viewer account is an ordinary `role=user` account and may own zero rooms.
- Keep the existing first-account bootstrap behavior because the unchanged Docker seed creates the initial super administrator through `/api/account/create`. The first account continues to receive its compatibility default room.
- Every later public registration creates an enabled `user` without a live room. This stops treating every viewer as a streamer while preserving existing deployments.
- `/api/account/create` returns a JWT so registration completes as a signed-in flow. Existing callers that ignore response data remain compatible.
- Existing users, roles, room ownership, and live rooms are unchanged.
- The frontend login page becomes a general account login/registration surface. A `redirect` query parameter returns users to the requested room or `/admin`; otherwise successful login/registration returns to `/`.
- Shared account actions on home and room pages expose login/registration, current username, admin/stream-management navigation, and logout.

## Database changes

Migration `09_add_live_room_privacy.sql` adds to `live_room`:

- `require_login BOOLEAN NOT NULL DEFAULT FALSE`
- `password_hash TEXT NOT NULL DEFAULT ''`
- `access_revision INTEGER NOT NULL DEFAULT 0`

The migration is idempotent and is registered in `src/db.rs::MIGRATIONS`. Existing rooms therefore remain public after upgrade.

No viewer-session or danmaku table is created. Password hashes use the existing salted PBKDF2-SHA256 implementation. Plaintext room passwords are never persisted, returned, logged, or placed in a media URL.

## Privacy semantics and management

Each room has two independent requirements:

| Require login | Has password | Access rule |
|---|---|---|
| No | No | Any guest or account may obtain a ticket |
| Yes | No | A valid enabled account is required |
| No | Yes | Guest or account must provide the correct room password |
| Yes | Yes | A valid enabled account and the correct password are both required |

- Private rooms remain discoverable in the live list and RSS behavior remains unchanged. Public metadata exposes `require_login` and `has_password`, never a hash.
- Room owners can update their own privacy settings. Admins can update room privacy through room management under existing role rules.
- Enabling password protection requires a valid password. Leaving the password field empty while protection is already enabled preserves the current password. Disabling protection clears the hash.
- Any change to login requirement, password enablement, or password value increments `access_revision` and immediately invalidates unused older tickets. Existing media connections are not forcibly disconnected.
- Password length is 6-64 Unicode scalar values. Login and room-password errors do not reveal stored credentials.

## Room metadata and access API

Add public endpoints:

- `GET /api/live/rooms/:stream_id` returns room title, cover, live status, `require_login`, `has_password`, and current viewer count.
- `POST /api/live/rooms/:stream_id/access` accepts `{ guest_id, password? }` and an optional account Bearer JWT, then returns `{ ticket, expires_at, viewer }`.
- `GET /api/live/rooms/:stream_id/ws?ticket=...` upgrades to the room WebSocket.

The guest ID is a browser-generated cryptographically random UUID persisted in local storage. The API validates its format and uses it only as a product-level identity key. A logged-in ticket ignores the guest ID and uses `user:<id>`; a guest ticket uses `guest:<uuid>`. Guest display names are deterministically derived as `游客-XXXX` without allowing impersonation through arbitrary nicknames.

Room-access claims include a dedicated token kind, stream ID, viewer key, display name, optional user ID, whether account and password checks passed, access revision, issued time, and a 15-minute expiry. Tickets use the configured auth secret but a distinct claims schema and explicit kind. Media URLs contain only the room ticket, not the account JWT or room password. An admitted connection may continue after ticket expiry; a reconnect obtains a new ticket by repeating the access request.

At ticket issuance and again at SRS/WebSocket admission, a centralized access policy checks the current room flags and revision. A ticket is rejected if expired, for another room, signed incorrectly, stale after a privacy change, or missing a requirement attestation.

## Viewer counting

“Currently watching” means unique platform viewer identities with at least one successfully admitted SRS playback session that has not stopped.

- `on_play` parses the `ticket` query parameter, validates room access, and records `client_id -> viewer_key` in the room hub.
- Duplicate callbacks for the same client are idempotent.
- The hub reference-counts sessions per viewer key. Multiple tabs/devices for the same logged account, or multiple tabs using the same guest ID, count once.
- `on_stop` removes by `stream_id + client_id`; duplicate or unknown stops do not decrement below zero.
- `on_unpublish` clears all playback sessions for that stream and broadcasts zero.
- A WebSocket connection alone never increases the count.
- Home polling reads hub counts in `PublicLiveRoom`; a room WebSocket receives the current count immediately and subsequent changes in real time.

The room hub is process memory protected by Tokio synchronization and uses `tokio::sync::broadcast` for room events. API restart resets counts to zero, and multiple API replicas would not share presence. This limitation is explicit and acceptable for the current single-instance Compose deployment.

## Danmaku protocol and validation

The same room WebSocket carries viewer-count and danmaku events. It requires a valid current room ticket but does not require an active media session.

Client message:

```json
{"type":"send_message","content":"hello"}
```

Server messages:

```json
{"type":"viewer_count","count":2}
{"type":"danmaku","id":"...","sender":{"kind":"guest","name":"游客-A1B2"},"content":"hello","sent_at":"..."}
{"type":"error","code":"rate_limited","message":"发送太快"}
```

- Content is trimmed, must contain 1-100 Unicode scalar values, and is always plain text.
- Each connection may send at most one accepted message per second. Rejected messages receive an error event and are not broadcast.
- The API generates message IDs and timestamps. Sender identity comes only from the ticket.
- Messages are broadcast live and are not stored or replayed after reconnect.
- Slow receivers may miss danmaku; they receive future events after lag recovery. Presence accuracy is independent of WebSocket delivery.
- Invalid tickets close with policy-violation semantics. Network disconnects trigger frontend reconnect with bounded backoff and ticket reacquisition when needed.

## Frontend behavior

### Home

- Show current viewer count on each live card.
- Show login-required and password-protected chips when enabled.
- Show shared signed-in/signed-out account actions.

### Login and registration

- One page supports both modes with username/password validation matching the API.
- Preserve a safe same-origin `redirect` parameter.
- Registration signs the viewer in directly.
- Logout clears the local JWT and returns to the public surface.

### Room access flow

1. Load public room metadata.
2. If login is required and no valid local JWT exists, show a login action preserving the room redirect.
3. If a password is required, show a password form; retain the value only in component memory.
4. Obtain the ticket, open WebSocket, and mount the player with ticketed URLs.
5. On access failure, retain the appropriate gate rather than briefly mounting an unauthorized player.

### Player and danmaku

- `MoyuPlayer` receives a room-access ticket instead of reading the account JWT itself.
- WHEP, HLS, and FLV URL builders append `ticket` safely.
- The room page owns the WebSocket. Viewer count appears near the player.
- Danmaku messages render as clipped, pointer-transparent right-to-left lanes above video and never cover controls. A recent-message text region remains accessible.
- With `prefers-reduced-motion`, animated copies become static overlays for six seconds.
- The composer displays the ticket identity, connection state, remaining character count, validation errors, and send button.

### Privacy controls

- Owner stream management adds a privacy card for the selected room.
- Admin room create/edit supports login requirement and password protection under existing permissions.
- Password fields are write-only: the UI can show “password configured” but never retrieve it.

The frontend root receives an extracted `DESIGN.md`. New work follows existing MUI dark, responsive stacks, semantic colors, bordered surfaces, visible focus states, and 4/8px spacing. `front/nginx.conf` adds WebSocket upgrade forwarding for `/api/` while preserving existing API limits and proxies.

## Error handling and security

- Media admission failures return SRS callback `code: 1`; no detailed credential reason is logged.
- API access failures use HTTP 401 for missing/invalid required account, 403 for password or stale-policy failures, 404 for unknown room, and 400 for malformed guest/message/password input.
- WebSocket ticket failures close without accepting messages.
- React renders content as text only. No HTML parsing or `dangerouslySetInnerHTML` is used.
- Ticket, JWT, stream code, room password, and password hash are excluded from application logs.
- Query tickets expire after 15 minutes and are room-scoped. This is playback admission control, not DRM: an admitted viewer can still record or share captured media.
- SRS WHEP Bearer handling is not trusted; query tickets are the authoritative hook input.

## Testing and acceptance scenarios

### Automated backend

- Migration runs on a fresh PostgreSQL database and reruns idempotently.
- Existing room defaults remain public.
- Ticket signature, expiry, room binding, access revision, and privacy matrix tests.
- Public registration: first-account bootstrap compatibility; later accounts are `user` with zero rooms.
- Hub tests: initial zero, duplicate play, unique identity deduplication, multi-client reference counting, duplicate stop, unknown stop, and unpublish cleanup.
- Danmaku tests: guest/account sender derivation, empty/over-100 rejection, plain text preservation, and rate limit.
- Existing callback, admin, live-room, and auth tests remain green.

### Automated frontend

- URL builders append encoded tickets for all playback protocols.
- Guest ID and redirect sanitization are deterministic and safe.
- Type checking, lint, and production build pass.

### Docker Compose real-surface scenarios

1. Seed super administrator logs in and creates/configures a room.
2. A later public registration logs in and owns no room.
3. All four privacy combinations admit/reject guest and account requests correctly.
4. A valid ticket admits playback; missing, expired, stale, or cross-room tickets are rejected by `on_play`.
5. Two playback clients sharing one identity count as one; another identity raises the count; stops decrement only after each identity's final session.
6. Guest and account clients exchange live danmaku; invalid and rate-limited messages do not broadcast.
7. Home and room surfaces show the count and privacy states at 375px, 768px, and 1280px without overflow.

### Quality gates

- `cargo fmt --all --check`
- `cargo clippy --locked --all-targets -- -D warnings`
- `cargo test --locked`
- `pnpm lint`
- `pnpm build`
- `docker compose -f docker-compose.test.yml up -d --build`, scenario verification, then cleanup with `down`

No git commit is part of autonomous execution. Repository writes require separate explicit permission under the active Commit Guard.
