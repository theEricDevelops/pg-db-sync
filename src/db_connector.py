import asyncio
import psycopg2
import psycopg2.extras

class DatabaseConnector:
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = None

    async def connect(self):
        try:
            self.connection = await asyncio.to_thread(psycopg2.connect, **self.db_config)
            print("Connection to database established.")
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise

    async def disconnect(self):
        if self.connection:
            await asyncio.to_thread(self.connection.close)
            print("Database connection closed.")

    async def execute_query(self, query, params=None):
        if not self.connection:
            raise Exception("Database connection is not established.")
        
        def _execute(conn, query, params):
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(query, params)
                if query.lower().startswith("select"):
                    return cursor.fetchall()
                else:
                    conn.commit()
                    return []

        return await asyncio.to_thread(_execute, self.connection, query, params)
