import { InvalidPersistedCollectionConfigError } from '@tanstack/db-sqlite-persisted-collection-core'
import type { SQLiteDriver } from '@tanstack/db-sqlite-persisted-collection-core'

export type CloudflareD1PreparedResultLike<T = Record<string, unknown>> = {
  results?: ReadonlyArray<T> | null
}

export type CloudflareD1PreparedStatementLike = {
  bind: (...params: ReadonlyArray<unknown>) => CloudflareD1PreparedStatementLike
  run: <T = Record<string, unknown>>() => Promise<
    CloudflareD1PreparedResultLike<T>
  >
}

export type CloudflareD1SessionLike = {
  prepare: (sql: string) => CloudflareD1PreparedStatementLike
  getBookmark?: () => Promise<string> | string
}

export type CloudflareD1DatabaseLike = {
  prepare: (sql: string) => CloudflareD1PreparedStatementLike
  exec?: (sql: string) => Promise<unknown>
  withSession?: (
    constraint?: `first-primary` | string,
  ) => CloudflareD1SessionLike
}

export type CloudflareD1SQLiteDriverOptions = {
  database: CloudflareD1DatabaseLike
}

function canUseDatabaseExec(sql: string): boolean {
  const normalized = sql.trim().toUpperCase()
  return (
    normalized.startsWith(`BEGIN`) ||
    normalized.startsWith(`COMMIT`) ||
    normalized.startsWith(`ROLLBACK`) ||
    normalized.startsWith(`SAVEPOINT`) ||
    normalized.startsWith(`RELEASE`)
  )
}

function normalizeExecSql(sql: string): string {
  const trimmedSql = sql.trim()
  if (trimmedSql.endsWith(`;`)) {
    return trimmedSql
  }
  return `${trimmedSql};`
}

function isUnsupportedSqlTransactionError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false
  }

  const message = error.message.toLowerCase()
  return (
    message.includes(`sql transactions disabled`) ||
    message.includes(`use the state.storage.transaction`) ||
    message.includes(`sql begin transaction`) ||
    message.includes(`savepoint statements`) ||
    message.includes(`begin immediate`)
  )
}

function createUnsupportedSqlTransactionError(
  operation: string,
  cause: unknown,
): InvalidPersistedCollectionConfigError {
  const message =
    cause instanceof Error && cause.message.length > 0
      ? cause.message
      : `unknown error`
  return new InvalidPersistedCollectionConfigError(
    `Cloudflare D1 driver requires SQL transaction support for "${operation}". ` +
      `This runtime rejected SQL transaction statements, so atomic transaction semantics cannot be guaranteed. ` +
      `Original error: ${message}`,
  )
}

function assertTransactionCallbackHasDriverArg(
  fn: (transactionDriver: SQLiteDriver) => Promise<unknown>,
): void {
  if (fn.length > 0) {
    return
  }

  throw new InvalidPersistedCollectionConfigError(
    `SQLiteDriver.transaction callback must accept the transaction driver argument`,
  )
}

function isD1DatabaseLike(value: unknown): value is CloudflareD1DatabaseLike {
  return (
    typeof value === `object` &&
    value !== null &&
    (typeof (value as CloudflareD1DatabaseLike).prepare === `function` ||
      typeof (value as CloudflareD1DatabaseLike).withSession === `function`)
  )
}

function normalizeD1Rows<T>(result: unknown, sql: string): ReadonlyArray<T> {
  if (Array.isArray(result)) {
    return result as ReadonlyArray<T>
  }

  if (result == null || typeof result !== `object`) {
    throw new InvalidPersistedCollectionConfigError(
      `Unsupported Cloudflare D1 query result shape for SQL "${sql}"`,
    )
  }

  const preparedResult = result as CloudflareD1PreparedResultLike<T>
  if (preparedResult.results == null) {
    return []
  }

  if (Array.isArray(preparedResult.results)) {
    return preparedResult.results
  }

  throw new InvalidPersistedCollectionConfigError(
    `Unsupported Cloudflare D1 query result shape for SQL "${sql}"`,
  )
}

function ensurePreparedStatementLike(
  statement: unknown,
  sql: string,
): CloudflareD1PreparedStatementLike {
  if (
    typeof statement !== `object` ||
    statement === null ||
    typeof (statement as CloudflareD1PreparedStatementLike).bind !==
      `function` ||
    typeof (statement as CloudflareD1PreparedStatementLike).run !== `function`
  ) {
    throw new InvalidPersistedCollectionConfigError(
      `Cloudflare D1 prepare("${sql}") must return a statement with bind/run methods`,
    )
  }

  return statement as CloudflareD1PreparedStatementLike
}

export class CloudflareD1SQLiteDriver implements SQLiteDriver {
  private readonly database: CloudflareD1DatabaseLike
  private readonly session: CloudflareD1SessionLike | null
  private queue: Promise<void> = Promise.resolve()
  private nextSavepointId = 1

  constructor(options: CloudflareD1SQLiteDriverOptions) {
    if (!isD1DatabaseLike(options.database)) {
      throw new InvalidPersistedCollectionConfigError(
        `Cloudflare D1 SQLite driver requires a database.prepare function`,
      )
    }

    this.database = options.database
    this.session =
      typeof options.database.withSession === `function`
        ? options.database.withSession(`first-primary`)
        : null
  }

  async exec(sql: string): Promise<void> {
    await this.enqueue(async () => {
      await this.executeExec(sql)
    })
  }

  async query<T>(
    sql: string,
    params: ReadonlyArray<unknown> = [],
  ): Promise<ReadonlyArray<T>> {
    return this.enqueue(async () => {
      const result = await this.executeRun<T>(sql, params)
      return normalizeD1Rows(result, sql)
    })
  }

  async run(sql: string, params: ReadonlyArray<unknown> = []): Promise<void> {
    await this.enqueue(async () => {
      await this.executeRun(sql, params)
    })
  }

  async transaction<T>(
    fn: (transactionDriver: SQLiteDriver) => Promise<T>,
  ): Promise<T> {
    assertTransactionCallbackHasDriverArg(fn)
    return this.transactionWithDriver(fn)
  }

  async transactionWithDriver<T>(
    fn: (transactionDriver: SQLiteDriver) => Promise<T>,
  ): Promise<T> {
    return this.enqueue(async () => {
      const transactionDriver = this.createTransactionDriver()
      try {
        await this.executeExec(`BEGIN IMMEDIATE`)
      } catch (error) {
        if (isUnsupportedSqlTransactionError(error)) {
          throw createUnsupportedSqlTransactionError(`BEGIN IMMEDIATE`, error)
        }
        throw error
      }

      try {
        const result = await fn(transactionDriver)
        await this.executeExec(`COMMIT`)
        return result
      } catch (error) {
        try {
          await this.executeExec(`ROLLBACK`)
        } catch {
          // Keep original transaction error.
        }
        throw error
      }
    })
  }

  getDatabase(): CloudflareD1DatabaseLike {
    return this.database
  }

  getSession(): CloudflareD1SessionLike | null {
    return this.session
  }

  private async executeExec(sql: string): Promise<void> {
    if (typeof this.database.exec === `function` && canUseDatabaseExec(sql)) {
      await this.database.exec(normalizeExecSql(sql))
      return
    }

    await this.executeRun(sql)
  }

  private async executeRun<T = Record<string, unknown>>(
    sql: string,
    params: ReadonlyArray<unknown> = [],
  ): Promise<unknown> {
    const preparedStatement = ensurePreparedStatementLike(
      this.prepareStatement(sql),
      sql,
    )
    const statementWithParams =
      params.length > 0 ? preparedStatement.bind(...params) : preparedStatement
    return statementWithParams.run<T>()
  }

  private prepareStatement(sql: string): CloudflareD1PreparedStatementLike {
    if (this.session) {
      return this.session.prepare(sql)
    }
    return this.database.prepare(sql)
  }

  private enqueue<T>(operation: () => Promise<T> | T): Promise<T> {
    const queuedOperation = this.queue.then(operation, operation)
    this.queue = queuedOperation.then(
      () => undefined,
      () => undefined,
    )
    return queuedOperation
  }

  private createTransactionDriver(): SQLiteDriver {
    const transactionDriver: SQLiteDriver = {
      exec: async (sql) => {
        await this.executeExec(sql)
      },
      query: async <T>(
        sql: string,
        params: ReadonlyArray<unknown> = [],
      ): Promise<ReadonlyArray<T>> => {
        const result = await this.executeRun<T>(sql, params)
        return normalizeD1Rows<T>(result, sql)
      },
      run: async (sql, params = []) => {
        await this.executeRun(sql, params)
      },
      transaction: async <T>(
        fn: (transactionDriver: SQLiteDriver) => Promise<T>,
      ): Promise<T> => {
        assertTransactionCallbackHasDriverArg(fn)
        return this.runNestedTransaction(transactionDriver, fn)
      },
      transactionWithDriver: async <T>(
        fn: (transactionDriver: SQLiteDriver) => Promise<T>,
      ): Promise<T> => this.runNestedTransaction(transactionDriver, fn),
    }

    return transactionDriver
  }

  private async runNestedTransaction<T>(
    transactionDriver: SQLiteDriver,
    fn: (transactionDriver: SQLiteDriver) => Promise<T>,
  ): Promise<T> {
    const savepointName = `tsdb_sp_${this.nextSavepointId}`
    this.nextSavepointId++
    try {
      await this.executeExec(`SAVEPOINT ${savepointName}`)
    } catch (error) {
      if (isUnsupportedSqlTransactionError(error)) {
        throw createUnsupportedSqlTransactionError(
          `SAVEPOINT ${savepointName}`,
          error,
        )
      }
      throw error
    }

    try {
      const result = await fn(transactionDriver)
      await this.executeExec(`RELEASE SAVEPOINT ${savepointName}`)
      return result
    } catch (error) {
      await this.executeExec(`ROLLBACK TO SAVEPOINT ${savepointName}`)
      await this.executeExec(`RELEASE SAVEPOINT ${savepointName}`)
      throw error
    }
  }
}

export function createCloudflareD1SQLiteDriver(
  options: CloudflareD1SQLiteDriverOptions,
): CloudflareD1SQLiteDriver {
  return new CloudflareD1SQLiteDriver(options)
}
