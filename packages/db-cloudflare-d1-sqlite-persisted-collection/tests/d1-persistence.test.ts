import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import {
  createCloudflareD1SQLitePersistence,
  persistedCollectionOptions,
} from '../src'
import { CloudflareD1SQLiteDriver } from '../src/d1-driver'
import { SingleProcessCoordinator } from '../../db-sqlite-persisted-collection-core/src'
import { runRuntimePersistenceContractSuite } from '../../db-sqlite-persisted-collection-core/tests/contracts/runtime-persistence-contract'
import { createBetterSqliteD1DatabaseHarness } from './helpers/better-sqlite-d1-database'
import type {
  RuntimePersistenceContractTodo,
  RuntimePersistenceDatabaseHarness,
} from '../../db-sqlite-persisted-collection-core/tests/contracts/runtime-persistence-contract'

function createRuntimeDatabaseHarness(): RuntimePersistenceDatabaseHarness {
  const tempDirectory = mkdtempSync(join(tmpdir(), `db-cf-d1-persistence-`))
  const dbPath = join(tempDirectory, `state.sqlite`)
  const activeDatabaseHarnesses = new Set<
    ReturnType<typeof createBetterSqliteD1DatabaseHarness>
  >()

  return {
    createDriver: () => {
      const databaseHarness = createBetterSqliteD1DatabaseHarness({
        filename: dbPath,
      })
      activeDatabaseHarnesses.add(databaseHarness)
      return new CloudflareD1SQLiteDriver({
        database: databaseHarness.database,
      })
    },
    cleanup: () => {
      for (const databaseHarness of activeDatabaseHarnesses) {
        try {
          databaseHarness.close()
        } catch {
          // ignore cleanup errors from already-closed handles
        }
      }
      activeDatabaseHarnesses.clear()
      rmSync(tempDirectory, { recursive: true, force: true })
    },
  }
}

runRuntimePersistenceContractSuite(`cloudflare d1 runtime helpers`, {
  createDatabaseHarness: createRuntimeDatabaseHarness,
  createAdapter: (driver) =>
    createCloudflareD1SQLitePersistence<RuntimePersistenceContractTodo, string>(
      {
        database: (driver as CloudflareD1SQLiteDriver).getDatabase(),
      },
    ).adapter,
  createPersistence: (driver, coordinator) =>
    createCloudflareD1SQLitePersistence<RuntimePersistenceContractTodo, string>(
      {
        database: (driver as CloudflareD1SQLiteDriver).getDatabase(),
        coordinator,
      },
    ),
  createCoordinator: () => new SingleProcessCoordinator(),
})

describe(`cloudflare d1 persistence helpers`, () => {
  it(`defaults coordinator to SingleProcessCoordinator`, () => {
    const runtimeHarness = createRuntimeDatabaseHarness()
    const driver = runtimeHarness.createDriver()

    try {
      const persistence = createCloudflareD1SQLitePersistence({
        database: (driver as CloudflareD1SQLiteDriver).getDatabase(),
      })
      expect(persistence.coordinator).toBeInstanceOf(SingleProcessCoordinator)
    } finally {
      runtimeHarness.cleanup()
    }
  })

  it(`infers mode from sync presence and keeps schema per collection`, async () => {
    const tempDirectory = mkdtempSync(join(tmpdir(), `db-cf-d1-schema-infer-`))
    const dbPath = join(tempDirectory, `state.sqlite`)
    const collectionId = `todos`
    const firstDatabaseHarness = createBetterSqliteD1DatabaseHarness({
      filename: dbPath,
    })
    const firstPersistence = createCloudflareD1SQLitePersistence<
      RuntimePersistenceContractTodo,
      string
    >({
      database: firstDatabaseHarness.database,
    })

    try {
      const firstCollectionOptions = persistedCollectionOptions<
        RuntimePersistenceContractTodo,
        string
      >({
        id: collectionId,
        schemaVersion: 1,
        getKey: (todo) => todo.id,
        persistence: firstPersistence,
      })
      await firstCollectionOptions.persistence.adapter.applyCommittedTx(
        collectionId,
        {
          txId: `tx-1`,
          term: 1,
          seq: 1,
          rowVersion: 1,
          mutations: [
            {
              type: `insert`,
              key: `1`,
              value: {
                id: `1`,
                title: `before mismatch`,
                score: 1,
              },
            },
          ],
        },
      )
    } finally {
      firstDatabaseHarness.close()
    }

    const secondDatabaseHarness = createBetterSqliteD1DatabaseHarness({
      filename: dbPath,
    })
    const secondPersistence = createCloudflareD1SQLitePersistence<
      RuntimePersistenceContractTodo,
      string
    >({
      database: secondDatabaseHarness.database,
    })
    try {
      const syncAbsentOptions = persistedCollectionOptions<
        RuntimePersistenceContractTodo,
        string
      >({
        id: collectionId,
        schemaVersion: 2,
        getKey: (todo) => todo.id,
        persistence: secondPersistence,
      })
      await expect(
        syncAbsentOptions.persistence.adapter.loadSubset(collectionId, {}),
      ).rejects.toThrow(`Schema version mismatch`)

      const syncPresentOptions = persistedCollectionOptions<
        RuntimePersistenceContractTodo,
        string
      >({
        id: collectionId,
        schemaVersion: 2,
        getKey: (todo) => todo.id,
        sync: {
          sync: ({ markReady }) => {
            markReady()
          },
        },
        persistence: secondPersistence,
      })
      const rows = await syncPresentOptions.persistence.adapter.loadSubset(
        collectionId,
        {},
      )
      expect(rows).toEqual([])
    } finally {
      secondDatabaseHarness.close()
      rmSync(tempDirectory, { recursive: true, force: true })
    }
  })
})
