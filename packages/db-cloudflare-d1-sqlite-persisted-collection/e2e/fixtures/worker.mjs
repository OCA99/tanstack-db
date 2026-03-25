// @ts-nocheck
import { createCollection } from '../../../db/dist/esm/index.js'
import {
  createCloudflareD1SQLitePersistence,
  persistedCollectionOptions,
} from '../../dist/esm/index.js'

const DEFAULT_COLLECTION_ID = `todos`
const DEFAULT_SCHEMA_VERSION = 1

function resolveCollectionPersistence({
  persistence,
  collectionId,
  syncEnabled,
  schemaVersion,
}) {
  const mode = syncEnabled ? `sync-present` : `sync-absent`
  return (
    persistence.resolvePersistenceForCollection?.({
      collectionId,
      mode,
      schemaVersion,
    }) ??
    persistence.resolvePersistenceForMode?.(mode) ??
    persistence
  )
}

function parseSyncEnabled(rawValue) {
  if (rawValue == null) {
    return false
  }

  const normalized = String(rawValue).toLowerCase()
  if (normalized === `1` || normalized === `true`) {
    return true
  }
  if (normalized === `0` || normalized === `false`) {
    return false
  }

  throw new Error(`Invalid PERSISTENCE_WITH_SYNC "${String(rawValue)}"`)
}

function parseSchemaVersion(rawSchemaVersion) {
  if (rawSchemaVersion == null) {
    return DEFAULT_SCHEMA_VERSION
  }
  const parsed = Number(rawSchemaVersion)
  if (Number.isInteger(parsed) && parsed >= 0) {
    return parsed
  }
  throw new Error(
    `Invalid PERSISTENCE_SCHEMA_VERSION "${String(rawSchemaVersion)}"`,
  )
}

function jsonResponse(status, body) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'content-type': 'application/json',
    },
  })
}

function serializeError(error) {
  if (error && typeof error === `object`) {
    const maybeCode = error.code
    return {
      name: typeof error.name === `string` ? error.name : `Error`,
      message:
        typeof error.message === `string`
          ? error.message
          : `Unknown Cloudflare D1 runtime error`,
      code: typeof maybeCode === `string` ? maybeCode : undefined,
    }
  }

  return {
    name: `Error`,
    message: `Unknown Cloudflare D1 runtime error`,
    code: undefined,
  }
}

function createUnknownCollectionError(collectionId) {
  const error = new Error(
    `Unknown cloudflare d1 persistence collection "${collectionId}"`,
  )
  error.name = `UnknownCloudflareD1PersistenceCollectionError`
  error.code = `UNKNOWN_COLLECTION`
  return error
}

function createRuntimeState(env) {
  const collectionId = env.PERSISTENCE_COLLECTION_ID ?? DEFAULT_COLLECTION_ID
  const syncEnabled = parseSyncEnabled(env.PERSISTENCE_WITH_SYNC)
  const schemaVersion = parseSchemaVersion(env.PERSISTENCE_SCHEMA_VERSION)
  const persistence = createCloudflareD1SQLitePersistence({
    database: env.DB,
  })
  const collectionPersistence = resolveCollectionPersistence({
    persistence,
    collectionId,
    syncEnabled,
    schemaVersion,
  })
  const ready = collectionPersistence.adapter.loadSubset(collectionId, {
    limit: 0,
  })

  const baseCollectionOptions = {
    id: collectionId,
    schemaVersion,
    getKey: (todo) => todo.id,
    persistence,
  }
  const collection = createCollection(
    syncEnabled
      ? persistedCollectionOptions({
          ...baseCollectionOptions,
          sync: {
            sync: ({ markReady }) => {
              markReady()
            },
          },
        })
      : persistedCollectionOptions(baseCollectionOptions),
  )

  return {
    collectionId,
    syncEnabled,
    schemaVersion,
    persistence,
    collectionPersistence,
    collection,
    ready,
    collectionReady: collection.stateWhenReady(),
  }
}

let runtimeState = null

function getRuntimeState(env) {
  if (runtimeState) {
    return runtimeState
  }

  runtimeState = createRuntimeState(env)
  return runtimeState
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url)

    try {
      if (request.method === `GET` && url.pathname === `/health`) {
        return jsonResponse(200, {
          ok: true,
        })
      }

      let requestBody = {}
      if (request.method === `POST`) {
        requestBody = await request.json()
      }
      const runtime = getRuntimeState(env)
      await runtime.ready

      if (request.method === `GET` && url.pathname === `/runtime-config`) {
        return jsonResponse(200, {
          ok: true,
          collectionId: runtime.collectionId,
          mode: runtime.syncEnabled ? `sync` : `local`,
          syncEnabled: runtime.syncEnabled,
          schemaVersion: runtime.schemaVersion,
        })
      }

      const collectionId = requestBody.collectionId ?? runtime.collectionId

      if (request.method === `POST` && url.pathname === `/write-todo`) {
        if (collectionId !== runtime.collectionId) {
          throw createUnknownCollectionError(collectionId)
        }
        if (runtime.syncEnabled) {
          const txId =
            typeof requestBody.txId === `string`
              ? requestBody.txId
              : crypto.randomUUID()
          const seq =
            typeof requestBody.seq === `number` ? requestBody.seq : Date.now()
          const rowVersion =
            typeof requestBody.rowVersion === `number`
              ? requestBody.rowVersion
              : seq
          await runtime.collectionPersistence.adapter.applyCommittedTx(
            collectionId,
            {
              txId,
              term: 1,
              seq,
              rowVersion,
              mutations: [
                {
                  type: `insert`,
                  key: requestBody.todo.id,
                  value: requestBody.todo,
                },
              ],
            },
          )

          return jsonResponse(200, {
            ok: true,
          })
        }
        await runtime.collectionReady
        const tx = runtime.collection.insert(requestBody.todo)
        await tx.isPersisted.promise

        return jsonResponse(200, {
          ok: true,
        })
      }

      if (request.method === `POST` && url.pathname === `/load-todos`) {
        if (collectionId !== runtime.collectionId) {
          throw createUnknownCollectionError(collectionId)
        }
        if (runtime.syncEnabled) {
          const rows = await runtime.collectionPersistence.adapter.loadSubset(
            collectionId,
            {},
          )
          return jsonResponse(200, {
            ok: true,
            rows: rows.map((row) => ({
              key: row.key,
              value: row.value,
            })),
          })
        }
        await runtime.collectionReady
        const rows = runtime.collection.toArray.map((todo) => ({
          key: todo.id,
          value: todo,
        }))
        return jsonResponse(200, {
          ok: true,
          rows,
        })
      }

      if (
        request.method === `POST` &&
        url.pathname === `/load-unknown-collection-error`
      ) {
        const unknownCollectionId = requestBody.collectionId ?? `missing`
        if (unknownCollectionId !== runtime.collectionId) {
          throw createUnknownCollectionError(unknownCollectionId)
        }
        const rows = await runtime.persistence.adapter.loadSubset(
          unknownCollectionId,
          {},
        )
        return jsonResponse(200, {
          ok: true,
          rows,
        })
      }

      return jsonResponse(404, {
        ok: false,
        error: {
          name: `NotFound`,
          message: `Unknown cloudflare d1 endpoint "${url.pathname}"`,
          code: `NOT_FOUND`,
        },
      })
    } catch (error) {
      return jsonResponse(500, {
        ok: false,
        error: serializeError(error),
      })
    }
  },
}
