import BetterSqlite3 from 'better-sqlite3'
import type {
  CloudflareD1DatabaseLike,
  CloudflareD1PreparedResultLike,
  CloudflareD1PreparedStatementLike,
} from '../../src/d1-driver'

type BetterSqliteD1DatabaseHarness = {
  database: CloudflareD1DatabaseLike
  close: () => void
}

type BetterSqliteStatement = ReturnType<BetterSqlite3.Database[`prepare`]>
type BetterSqliteStatementWithVariadics = BetterSqliteStatement & {
  all: (...params: ReadonlyArray<unknown>) => ReadonlyArray<unknown>
  run: (...params: ReadonlyArray<unknown>) => unknown
}

class BetterSqliteD1PreparedStatement implements CloudflareD1PreparedStatementLike {
  private readonly sql: string
  private readonly database: BetterSqlite3.Database
  private readonly boundParams: ReadonlyArray<unknown>

  constructor(options: {
    sql: string
    database: BetterSqlite3.Database
    boundParams?: ReadonlyArray<unknown>
  }) {
    this.sql = options.sql
    this.database = options.database
    this.boundParams = options.boundParams ?? []
  }

  bind(...params: ReadonlyArray<unknown>): CloudflareD1PreparedStatementLike {
    return new BetterSqliteD1PreparedStatement({
      sql: this.sql,
      database: this.database,
      boundParams: params,
    })
  }

  async run<T = Record<string, unknown>>(): Promise<
    CloudflareD1PreparedResultLike<T>
  > {
    const statement = this.database.prepare(this.sql)
    if (statement.reader) {
      return {
        results: readRows<T>(statement, this.boundParams),
      }
    }

    runStatement(statement, this.boundParams)
    return {
      results: [],
    }
  }
}

function readRows<T>(
  statement: BetterSqliteStatement,
  params: ReadonlyArray<unknown>,
): ReadonlyArray<T> {
  const statementWithVariadics =
    statement as BetterSqliteStatementWithVariadics
  if (params.length === 0) {
    return statementWithVariadics.all() as ReadonlyArray<T>
  }
  return statementWithVariadics.all(...params) as ReadonlyArray<T>
}

function runStatement(
  statement: BetterSqliteStatement,
  params: ReadonlyArray<unknown>,
): void {
  const statementWithVariadics =
    statement as BetterSqliteStatementWithVariadics
  if (params.length === 0) {
    statementWithVariadics.run()
    return
  }
  statementWithVariadics.run(...params)
}

export function createBetterSqliteD1DatabaseHarness(options: {
  filename: string
}): BetterSqliteD1DatabaseHarness {
  const db = new BetterSqlite3(options.filename)

  const database: CloudflareD1DatabaseLike = {
    prepare: (sql) =>
      new BetterSqliteD1PreparedStatement({
        sql,
        database: db,
      }),
    exec: async (sql) => {
      db.exec(sql)
      return {
        count: 1,
        duration: 0,
      }
    },
  }

  return {
    database,
    close: () => {
      db.close()
    },
  }
}
