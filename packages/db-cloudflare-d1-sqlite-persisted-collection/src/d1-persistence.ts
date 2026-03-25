import {
  SingleProcessCoordinator,
  createSQLiteCorePersistenceAdapter,
} from '@tanstack/db-sqlite-persisted-collection-core'
import { CloudflareD1SQLiteDriver } from './d1-driver'
import type {
  PersistedCollectionCoordinator,
  PersistedCollectionMode,
  PersistedCollectionPersistence,
  SQLiteCoreAdapterOptions,
  SQLiteDriver,
} from '@tanstack/db-sqlite-persisted-collection-core'
import type { CloudflareD1DatabaseLike } from './d1-driver'

export type { CloudflareD1DatabaseLike } from './d1-driver'

type CloudflareD1CoreSchemaMismatchPolicy =
  | `sync-present-reset`
  | `sync-absent-error`
  | `reset`

export type CloudflareD1SchemaMismatchPolicy =
  | CloudflareD1CoreSchemaMismatchPolicy
  | `throw`

type CloudflareD1SQLitePersistenceBaseOptions = Omit<
  SQLiteCoreAdapterOptions,
  `driver` | `schemaVersion` | `schemaMismatchPolicy`
> & {
  database: CloudflareD1DatabaseLike
  coordinator?: PersistedCollectionCoordinator
  schemaMismatchPolicy?: CloudflareD1SchemaMismatchPolicy
}

export type CloudflareD1SQLitePersistenceOptions =
  CloudflareD1SQLitePersistenceBaseOptions

function normalizeSchemaMismatchPolicy(
  policy: CloudflareD1SchemaMismatchPolicy,
): CloudflareD1CoreSchemaMismatchPolicy {
  if (policy === `throw`) {
    return `sync-absent-error`
  }
  return policy
}

function resolveSchemaMismatchPolicy(
  explicitPolicy: CloudflareD1SchemaMismatchPolicy | undefined,
  mode: PersistedCollectionMode,
): CloudflareD1CoreSchemaMismatchPolicy {
  if (explicitPolicy) {
    return normalizeSchemaMismatchPolicy(explicitPolicy)
  }

  return mode === `sync-present` ? `sync-present-reset` : `sync-absent-error`
}

function createAdapterCacheKey(
  schemaMismatchPolicy: CloudflareD1CoreSchemaMismatchPolicy,
  schemaVersion: number | undefined,
): string {
  const schemaVersionKey =
    schemaVersion === undefined ? `schema:default` : `schema:${schemaVersion}`
  return `${schemaMismatchPolicy}|${schemaVersionKey}`
}

function resolveSQLiteDriver(
  options: CloudflareD1SQLitePersistenceOptions,
): SQLiteDriver {
  return new CloudflareD1SQLiteDriver({
    database: options.database,
  })
}

function resolveAdapterBaseOptions(
  options: CloudflareD1SQLitePersistenceOptions,
): Omit<
  SQLiteCoreAdapterOptions,
  `driver` | `schemaVersion` | `schemaMismatchPolicy`
> {
  return {
    appliedTxPruneMaxRows: options.appliedTxPruneMaxRows,
    appliedTxPruneMaxAgeSeconds: options.appliedTxPruneMaxAgeSeconds,
    pullSinceReloadThreshold: options.pullSinceReloadThreshold,
  }
}

/**
 * Creates a shared Cloudflare D1 SQLite persistence instance that can be
 * reused by many collections in one D1 database.
 */
export function createCloudflareD1SQLitePersistence<
  T extends object,
  TKey extends string | number = string | number,
>(
  options: CloudflareD1SQLitePersistenceOptions,
): PersistedCollectionPersistence<T, TKey> {
  const { coordinator, schemaMismatchPolicy } = options
  const driver = resolveSQLiteDriver(options)
  const adapterBaseOptions = resolveAdapterBaseOptions(options)
  const resolvedCoordinator = coordinator ?? new SingleProcessCoordinator()
  const adapterCache = new Map<
    string,
    ReturnType<
      typeof createSQLiteCorePersistenceAdapter<
        Record<string, unknown>,
        string | number
      >
    >
  >()

  const getAdapterForCollection = (
    mode: PersistedCollectionMode,
    schemaVersion: number | undefined,
  ) => {
    const resolvedSchemaMismatchPolicy = resolveSchemaMismatchPolicy(
      schemaMismatchPolicy,
      mode,
    )
    const cacheKey = createAdapterCacheKey(
      resolvedSchemaMismatchPolicy,
      schemaVersion,
    )
    const cachedAdapter = adapterCache.get(cacheKey)
    if (cachedAdapter) {
      return cachedAdapter
    }

    const adapter = createSQLiteCorePersistenceAdapter<
      Record<string, unknown>,
      string | number
    >({
      ...adapterBaseOptions,
      driver,
      schemaMismatchPolicy: resolvedSchemaMismatchPolicy,
      ...(schemaVersion === undefined ? {} : { schemaVersion }),
    })
    adapterCache.set(cacheKey, adapter)
    return adapter
  }

  const createCollectionPersistence = (
    mode: PersistedCollectionMode,
    schemaVersion: number | undefined,
  ): PersistedCollectionPersistence<T, TKey> => ({
    adapter: getAdapterForCollection(
      mode,
      schemaVersion,
    ) as unknown as PersistedCollectionPersistence<T, TKey>[`adapter`],
    coordinator: resolvedCoordinator,
  })

  const defaultPersistence = createCollectionPersistence(
    `sync-absent`,
    undefined,
  )

  return {
    ...defaultPersistence,
    resolvePersistenceForCollection: ({ mode, schemaVersion }) =>
      createCollectionPersistence(mode, schemaVersion),
    // Backward compatible fallback for older callers.
    resolvePersistenceForMode: (mode) =>
      createCollectionPersistence(mode, undefined),
  }
}
