import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import { runSQLiteDriverContractSuite } from '../../db-sqlite-persisted-collection-core/tests/contracts/sqlite-driver-contract'
import { CloudflareD1SQLiteDriver } from '../src/d1-driver'
import { createBetterSqliteD1DatabaseHarness } from './helpers/better-sqlite-d1-database'
import type { SQLiteDriverContractHarness } from '../../db-sqlite-persisted-collection-core/tests/contracts/sqlite-driver-contract'

function createDriverHarness(): SQLiteDriverContractHarness {
  const tempDirectory = mkdtempSync(join(tmpdir(), `db-cf-d1-driver-`))
  const dbPath = join(tempDirectory, `state.sqlite`)
  const databaseHarness = createBetterSqliteD1DatabaseHarness({
    filename: dbPath,
  })
  const driver = new CloudflareD1SQLiteDriver({
    database: databaseHarness.database,
  })

  return {
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

runSQLiteDriverContractSuite(`cloudflare d1 sqlite driver`, createDriverHarness)

describe(`cloudflare d1 sqlite driver`, () => {
  it(`falls back to non-SQL transaction mode when SQL transaction statements are blocked`, async () => {
    const executedSql = new Array<string>()
    const driver = new CloudflareD1SQLiteDriver({
      database: {
        prepare: (sql) => ({
          bind: (..._params) => ({
            bind: () => {
              throw new Error(`unexpected nested bind`)
            },
            run: () => {
              executedSql.push(sql)
              if (
                sql.startsWith(`BEGIN`) ||
                sql === `COMMIT` ||
                sql === `ROLLBACK`
              ) {
                throw new Error(`sql transactions disabled`)
              }
              return Promise.resolve({ results: [] })
            },
          }),
          run: () => {
            executedSql.push(sql)
            if (
              sql.startsWith(`BEGIN`) ||
              sql === `COMMIT` ||
              sql === `ROLLBACK`
            ) {
              throw new Error(`sql transactions disabled`)
            }
            return Promise.resolve({ results: [] })
          },
        }),
      },
    })

    await driver.transaction(async (transactionDriver) => {
      await transactionDriver.run(`INSERT INTO todos (id) VALUES (?)`, [`1`])
    })

    expect(executedSql).toContain(`BEGIN IMMEDIATE`)
    expect(executedSql).toContain(`INSERT INTO todos (id) VALUES (?)`)
  })

  it(`uses database.exec when available`, async () => {
    const executedSql = new Array<string>()
    const fallbackRuns = new Array<string>()
    const driver = new CloudflareD1SQLiteDriver({
      database: {
        prepare: (sql) => ({
          bind: () => ({
            bind: () => {
              throw new Error(`unexpected nested bind`)
            },
            run: () => {
              fallbackRuns.push(sql)
              return Promise.resolve({ results: [] })
            },
          }),
          run: () => {
            fallbackRuns.push(sql)
            return Promise.resolve({ results: [] })
          },
        }),
        exec: async (sql) => {
          executedSql.push(sql)
        },
      },
    })

    await driver.exec(`CREATE TABLE test_exec (id TEXT PRIMARY KEY)`)

    expect(executedSql).toEqual([])
    expect(fallbackRuns).toEqual([
      `CREATE TABLE test_exec (id TEXT PRIMARY KEY)`,
    ])
  })

  it(`handles null results as an empty query result set`, async () => {
    const driver = new CloudflareD1SQLiteDriver({
      database: {
        prepare: () => ({
          bind: () => ({
            bind: () => {
              throw new Error(`unexpected nested bind`)
            },
            run: () => Promise.resolve({ results: null }),
          }),
          run: () => Promise.resolve({ results: null }),
        }),
      },
    })

    const rows = await driver.query<{ id: string }>(`SELECT id FROM todos`)
    expect(rows).toEqual([])
  })

  it(`throws a clear error for unsupported D1 result shapes`, async () => {
    const driver = new CloudflareD1SQLiteDriver({
      database: {
        prepare: () => ({
          bind: (...params) => ({
            bind: () => {
              throw new Error(`unexpected nested bind`)
            },
            run: () => {
              if (params.length > 0) {
                return Promise.resolve({ results: [] })
              }
              return Promise.resolve({ results: [] })
            },
          }),
          run: () =>
            Promise.resolve({
              // intentionally invalid result shape
              results: { value: 1 } as unknown as ReadonlyArray<unknown>,
            } as never),
        }),
      },
    })

    await expect(
      driver.query<{ id: string }>(`SELECT id FROM todos`),
    ).rejects.toThrow(`Unsupported Cloudflare D1 query result shape`)
  })
})
