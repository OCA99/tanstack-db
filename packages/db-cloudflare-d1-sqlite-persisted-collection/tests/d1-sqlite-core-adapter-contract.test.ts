import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { runSQLiteCoreAdapterContractSuite } from '../../db-sqlite-persisted-collection-core/tests/contracts/sqlite-core-adapter-contract'
import { CloudflareD1SQLiteDriver } from '../src/d1-driver'
import { SQLiteCorePersistenceAdapter } from '../../db-sqlite-persisted-collection-core/src'
import { createBetterSqliteD1DatabaseHarness } from './helpers/better-sqlite-d1-database'
import type {
  SQLiteCoreAdapterContractTodo,
  SQLiteCoreAdapterHarnessFactory,
} from '../../db-sqlite-persisted-collection-core/tests/contracts/sqlite-core-adapter-contract'

const createHarness: SQLiteCoreAdapterHarnessFactory = (options) => {
  const tempDirectory = mkdtempSync(join(tmpdir(), `db-cf-d1-sql-core-`))
  const dbPath = join(tempDirectory, `state.sqlite`)
  const databaseHarness = createBetterSqliteD1DatabaseHarness({
    filename: dbPath,
  })
  const driver = new CloudflareD1SQLiteDriver({
    database: databaseHarness.database,
  })
  const adapter = new SQLiteCorePersistenceAdapter<
    SQLiteCoreAdapterContractTodo,
    string
  >({
    driver,
    ...options,
  })

  return {
    adapter,
    driver,
    cleanup: () => {
      try {
        databaseHarness.close()
      } finally {
        rmSync(tempDirectory, { recursive: true, force: true })
      }
    },
  }
}

runSQLiteCoreAdapterContractSuite(
  `SQLiteCorePersistenceAdapter (cloudflare d1 sqlite driver harness)`,
  createHarness,
)
