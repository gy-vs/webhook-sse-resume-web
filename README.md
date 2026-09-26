# webhook-sse-resume-web

Webhook 调试台：按工作区（workspace）接收 webhook，并通过 SSE（Server-Sent Events）实时推送。

## 事件编号与续传

- 服务端为每个工作区维护**单调递增**的事件序号 `seq`，并在启动时生成一个 `epoch`（UUID）。事件的全局身份是 `(epoch, seq)`，SSE 帧的 `id` 为 `epoch:seq`。
- 每个工作区维护有界重放缓冲（默认 100 条，可用 `createApp({bufferCapacity})` 覆盖）。
- 重连时浏览器自动携带 `Last-Event-ID`，服务端**只补发严格在其后**的同 epoch 事件（`seq > cursor.seq`）。
- 当游标早于缓冲区最老记录时，先补发仍保留的尾部事件，并发送一个明确的 `event: gap` 帧（`buffer-overflow`），而不是假装连续。
- 游标 epoch 与当前 epoch 不同（服务端重启）时发送 `epoch-changed` gap，再补发新 epoch 的缓冲；游标领先于缓冲时发送 `ahead-of-buffer`。
- `gap` / `ready` 帧不携带 `id`，陈旧游标不会被推进到缺口之后。

## 前端

- 按 `(epoch, seq)` 去重：断线重连的重复补发不会重复显示；不同 epoch 的同序号事件视为不同事件，分段展示、绝不合并。
- 收到 gap 时保留所有已显示事件，在列表对应位置插入缺口提示（不重置列表）。
- 切换工作区或 React StrictMode 重挂时关闭旧的 `EventSource`，任意时刻每个工作区只有一条连接。
- 未读数按工作区统计，标签页回到前台或切回该工作区时清零。

## API

- `POST /api/capture/:workspace/*path` — 接收 webhook，返回 `{epoch, seq}`。
- `GET  /api/stream/:workspace` — SSE；可用 `Last-Event-ID: epoch:seq` 续传。
- `GET  /api/events?workspace=name` — 当前缓冲快照（`{epoch, events}`）。
- `GET  /api/bootstrap?workspace=name` — `{kind, count, epoch, bufferCapacity}`。
- `POST /api/replay` — 重放指定 id。

## 开发

```bash
npm install
npm run dev    # API on :4174, Vite on :4173
npm test       # vitest（SSE 续传 / 溢出 / 双标签页 / 工作区切换 / epoch 重启等）
npm run build  # tsc 类型检查 + vite 构建
```
