import psycopg2
from psycopg2 import sql
from psycopg2 import errors
from psycopg2.extras import execute_values
import pandas as pd

class PostgresClient:
    def __init__(self, host: str, port: int, database: str, user: str, password: str, schemas: str) -> None:
        self.conn = None
        self.cursor = None
        self._credentials = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
            "options": f"-c search_path={schemas}"
        }

    @classmethod
    def from_credentials(cls, host: str, port: int, database: str, user: str, password: str, schemas: str) -> "PostgresClient":
        """Create an instance and connect immediately."""
        instance = cls(host, port, database, user, password, schemas)
        instance.connect()
        return instance

    def connect(self) -> None:
        """Establishes the database connection and cursor."""
        try:
            self.conn = psycopg2.connect(**self._credentials)
            self.cursor = self.conn.cursor()
            print("✅ Connected to PostgreSQL.")
        except Exception as e:
            raise ConnectionError(f"❌ Connection failed: {e}")

    def list_schemas(self) -> list:
        """
        Returns a list of all schema names in the current database.
        You can filter out system schemas if you like.
        """
        sql = """
        SELECT schema_name
          FROM information_schema.schemata
        ORDER BY schema_name;
        """
        self.cursor.execute(sql)
        return [row[0] for row in self.cursor.fetchall()]

    def list_tables(self, schema: str = 'public') -> list:
        """Returns a list of table names in the given schema."""
        # Ensure schema name is safely quoted
        schema_escaped = schema.replace("'", "''")
        sql = (
            "SELECT table_name"
            " FROM information_schema.tables"
            f" WHERE table_schema = '{schema_escaped}';"
        )
        self.cursor.execute(sql)
        return [row[0] for row in self.cursor.fetchall()]
    
    def run_query(self, sql: str) -> pd.DataFrame:
        # always clear any prior aborted transaction
        try:
            self.conn.rollback()
        except Exception:
            pass

        try:
            self.cursor.execute(sql)
        except errors.InFailedSqlTransaction:
            # clear the bad transaction then retry once
            self.conn.rollback()
            self.cursor.execute(sql)
        except Exception as e:
            # any other error: roll back and re-raise
            self.conn.rollback()
            raise RuntimeError(f"❌ Query failed: {e}")

        # now fetch results
        cols = [desc[0] for desc in self.cursor.description]
        rows = self.cursor.fetchall()
        return pd.DataFrame(rows, columns=cols)

    def insert_dataframe(self, table: str, df: pd.DataFrame) -> None:
        """
        Bulk-inserts a DataFrame into the specified schema.table.
        Uses psycopg2.sql to safely quote identifiers.
        """
        try:
            # split schema.table
            schema, tbl = table.split(".", 1)

            # build a list of Identifier objects for each column
            identifiers = [sql.Identifier(col) for col in df.columns]

            # build the INSERT statement with placeholders for the VALUES block
            template = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
                sql.Identifier(schema),
                sql.Identifier(tbl),
                sql.SQL(", ").join(identifiers)
            )

            # extract row tuples
            values = [tuple(row) for row in df.to_numpy()]

            # execute_values handles the bulk insert efficiently
            execute_values(self.cursor, template, values)
            self.conn.commit()
            print(f"✅ Inserted {len(values)} rows into '{schema}.{tbl}'.")
        except Exception as e:
            self.conn.rollback()
            raise RuntimeError(f"❌ Insert {tbl} failed: {e}")

    def close(self) -> None:
        """Closes the cursor and connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("🔒 PostgreSQL connection closed.")
