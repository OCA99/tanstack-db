---
'@tanstack/db-cloudflare-d1-sqlite-persisted-collection': patch
---

feat(persistence): add Cloudflare D1 SQLite persisted collection runtime package

- Introduce `@tanstack/db-cloudflare-d1-sqlite-persisted-collection`
- Add async D1-backed `SQLiteDriver` implementation compatible with the shared SQLite persistence core
- Add Cloudflare D1 persistence wrapper with collection-mode/schema-aware adapter resolution
- Add driver/core/runtime contract coverage and wrangler local runtime-bridge e2e tests
