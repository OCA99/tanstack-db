# @tanstack/db-cloudflare-d1-sqlite-persisted-collection

Thin SQLite persistence for Cloudflare D1.

## Public API

- `createCloudflareD1SQLitePersistence(...)`
- `persistedCollectionOptions(...)` (re-exported from core)

## Quick start

```ts
import { createCollection } from '@tanstack/db'
import {
  createCloudflareD1SQLitePersistence,
  persistedCollectionOptions,
} from '@tanstack/db-cloudflare-d1-sqlite-persisted-collection'

type Todo = {
  id: string
  title: string
  completed: boolean
}

export default {
  async fetch(request: Request, env: { DB: D1Database }) {
    const persistence = createCloudflareD1SQLitePersistence<Todo, string>({
      database: env.DB,
    })

    const todos = createCollection(
      persistedCollectionOptions<Todo, string>({
        id: `todos`,
        getKey: (todo) => todo.id,
        persistence,
        schemaVersion: 1,
      }),
    )

    await todos.stateWhenReady()
    return Response.json(todos.toArray)
  },
}
```

## Notes

- One shared persistence instance can serve multiple collections.
- Mode defaults are inferred from collection usage:
  - sync config present => `sync-present-reset`
  - no sync config => `sync-absent-error`
- You can still override with `schemaMismatchPolicy` if needed.
