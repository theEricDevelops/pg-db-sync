import asyncio
import psycopg2
import psycopg2.extras
from typing import List, Dict, Any, Tuple
from utils.logger import Logger

logger = Logger("db_utils")

class DatabaseUtility:
    def __init__(self, db_config: Dict[str, Any]):
        """
        Initializes the DatabaseUtility with database connection details.

        Args:
            db_config (Dict[str, Any]): A dictionary containing database connection parameters.
        """
        self.db_config = db_config
        self.connection = None

    async def connect(self) -> None:
        """Establishes a connection to the database."""
        try:
            connection_params = {
                "dbname": self.db_config["dbname"],
                "user": self.db_config["user"],
                "password": self.db_config["password"],
                "host": self.db_config["host"],
                "port": self.db_config["port"],
            }
            self.connection = await asyncio.to_thread(psycopg2.connect, **connection_params)
            logger.info(f"Connected to database: {self.db_config['dbname']} on {self.db_config['host']}")
            logger.debug(f"Connection: {self.connection}")

        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            raise

    async def disconnect(self) -> None:
        """Closes the database connection."""
        if self.connection:
            await asyncio.to_thread(self.connection.close)
            logger.info("Database connection closed.")

    async def execute_query(self, query: str, params: Tuple = None) -> List[Dict]:
        """
        Executes a SQL query and returns the results.

        Args:
            query (str): The SQL query to execute.
            params (Tuple, optional): Parameters to pass to the query. Defaults to None.

        Returns:
            List[Dict]: A list of dictionaries representing the query results.
        """
        if not self.connection:
            raise Exception("Database connection is not established.")

        def _execute(conn, query, params):
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                try:
                    cursor.execute(query, params)
                    if query.lower().startswith("select"):
                        return cursor.fetchall()
                    else:
                        conn.commit()
                        return []
                except Exception as e:
                    logger.error(f"Error executing query: {query} with params: {params}. Error: {e}")
                    conn.rollback()
                    raise

        return await asyncio.to_thread(_execute, self.connection, query, params)

    async def fetch_all_tables(self, schema: str = "public") -> List[str]:
        """
        Fetches a list of all tables in the specified schema.

        Args:
            schema (str, optional): The schema to search in. Defaults to "public".

        Returns:
            List[str]: A list of table names.
        """
        logger.info(f"Fetching tables in schema '{schema}'")
        try:
            # Start with the raw connection query that was successful
            logger.debug(f"Using raw connection to query tables in schema '{schema}'")
            
            def _raw_execute(conn, schema):
                with conn.cursor() as cursor:
                    cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname = %s", (schema,))
                    return [row[0] for row in cursor.fetchall()]
            
            tables = await asyncio.to_thread(_raw_execute, self.connection, schema)
            logger.debug(f"Raw connection query found tables: {tables}")
            
            if tables:
                logger.info(f"Found {len(tables)} tables in schema '{schema}'")
                return tables
                
            # If the primary approach fails, try these fallbacks
            logger.debug("Primary query returned no tables, checking search_path and permissions")
            
            # Check search_path settings
            search_path_query = "SHOW search_path;"
            search_path_result = await self.execute_query(search_path_query)
            logger.debug(f"Current search_path: {search_path_result}")
            
            # Check current user and their permissions
            user_query = "SELECT current_user, current_database();"
            user_result = await self.execute_query(user_query)
            logger.debug(f"Current user and database: {user_result}")
            
            # Try alternative query if needed
            logger.debug("Attempting to list visible tables without schema filtering")
            visible_tables_query = """
                SELECT c.relname AS table_name
                FROM pg_catalog.pg_class c
                LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind = 'r'
                AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                AND c.relname !~ '^pg_';
            """
            visible_result = await self.execute_query(visible_tables_query)
            logger.debug(f"Visible tables without schema filter: {visible_result}")
            
            if visible_result:
                tables = [row['table_name'] for row in visible_result]
                logger.warning(f"Using unfiltered table list: {tables}")
                return tables
            
            logger.warning(f"No tables found in schema '{schema}' after multiple attempts")
            return []
            
        except Exception as e:
            logger.error(f"Error fetching tables: {str(e)}")
            # Try emergency query as last resort
            try:
                emergency_query = """
                    SELECT tablename FROM pg_tables 
                    WHERE tableowner = current_user;
                """
                logger.debug(f"Attempting emergency query to list tables owned by current user")
                result = await self.execute_query(emergency_query)
                if result:
                    tables = [row['tablename'] for row in result]
                    logger.debug(f"Emergency query found tables: {tables}")
                    return tables
            except Exception as inner_e:
                logger.error(f"Emergency query also failed: {inner_e}")
            return []

    async def fetch_table_data(self, table_name: str) -> List[Dict]:
        """
        Fetches all data from a specified table.

        Args:
            table_name (str): The name of the table to fetch data from.

        Returns:
            List[Dict]: A list of dictionaries representing the table data.
        """
        query = f"SELECT * FROM {table_name};"
        return await self.execute_query(query)

    async def create_table(self, table_name: str, columns: List[Dict[str, str]]) -> None:
        """
        Creates a table with the specified name and columns.

        Args:
            table_name (str): The name of the table to create.
            columns (List[Dict[str, str]]): A list of column definitions, each a dictionary with 'name' and 'type'.
        """
        column_definitions = ", ".join([f"{col['name']} {col['type']}" for col in columns])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_definitions});"
        await self.execute_query(query)
        logger.info(f"Table '{table_name}' created successfully.")

    async def insert_data(self, table_name: str, data: List[Dict]) -> None:
        """
        Inserts data into a specified table.

        Args:
            table_name (str): The name of the table to insert data into.
            data (List[Dict]): A list of dictionaries, each representing a row of data.
        """
        if not data:
            logger.warning(f"No data to insert into table '{table_name}'.")
            return

        try:
            # Convert the keys to a list to ensure it's not an iterator
            columns = list(data[0].keys())
            logger.debug(f"Columns for insert into '{table_name}': {columns}")
            
            placeholders = ", ".join(["%s"] * len(columns))
            column_names = ", ".join(columns)
            query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders});"

            total_inserts = len(data)
            logger.info(f"Inserting {total_inserts} rows into table '{table_name}'")
            insert_count = 0
            for row in data:
                # Convert values to a list in the same order as columns
                values = [row[col] for col in columns]
                await self.execute_query(query, tuple(values))
                insert_count += 1
                
                # Log progress for large inserts
                if insert_count % 100 == 0:
                    logger.debug(f"Inserted {insert_count} rows of {total_inserts} into table '{table_name}' so far...")
                    
            logger.info(f"Inserted {insert_count} rows into table '{table_name}'.")
        except Exception as e:
            logger.error(f"Error inserting data into table '{table_name}': {str(e)}")
            # Add specific handling for the odict_iterator error
            if "odict_iterator" in str(e):
                logger.debug(f"Data format issue detected. First row sample: {str(data[0])[:200]}...")
                
                # Try alternative approach with explicit column extraction
                try:
                    logger.debug(f"Attempting alternative insertion approach for table '{table_name}'")
                    # Get columns from the first item's keys
                    sample_row = next(iter(data))
                    columns = list(sample_row.keys())
                    
                    # Create parametrized query
                    column_names = ", ".join(columns)
                    placeholders = ", ".join(["%s"] * len(columns))
                    query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders});"
                    
                    # Insert data with explicit value extraction
                    count = 0
                    for row in data:
                        values = tuple(row.get(col) for col in columns)
                        await self.execute_query(query, values)
                        count += 1
                    
                    logger.info(f"Alternative approach: Inserted {count} rows into table '{table_name}'.")
                except Exception as alt_e:
                    logger.error(f"Alternative insertion also failed: {str(alt_e)}")

    async def table_exists(self, table_name: str, schema: str = "public") -> bool:
        """
        Checks if a table exists in the specified schema.

        Args:
            table_name (str): The name of the table to check.
            schema (str, optional): The schema to check in. Defaults to "public".

        Returns:
            bool: True if the table exists, False otherwise.
        """
        try:
            # Use raw connection query to check table existence
            def _raw_table_check(conn, schema, table_name):
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM pg_tables WHERE schemaname = %s AND tablename = %s", 
                                  (schema, table_name))
                    result = cursor.fetchone()
                    return result[0] > 0
            
            logger.debug(f"Checking if table '{table_name}' exists in schema '{schema}'")
            exists = await asyncio.to_thread(_raw_table_check, self.connection, schema, table_name)
            logger.debug(f"Table '{table_name}' exists: {exists}")
            return exists
            
        except Exception as e:
            logger.error(f"Error checking if table exists: {str(e)}")
            # Fallback to information_schema query if the raw approach fails
            try:
                query = """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = %s
                        AND table_name = %s
                    );
                """
                result = await self.execute_query(query, (schema, table_name))
                return result[0]['exists']
            except Exception as inner_e:
                logger.error(f"Fallback query also failed: {inner_e}")
                return False

    async def get_table_columns(self, table_name: str, schema: str = "public") -> List[Dict]:
        """
        Gets the column names and types for a specified table.

        Args:
            table_name (str): The name of the table.
            schema (str, optional): The schema to check in. Defaults to "public".

        Returns:
            List[Dict]: A list of dictionaries, each with 'name' and 'type' keys.
        """
        logger.debug(f"Fetching columns for table '{table_name}' in schema '{schema}'")
        try:
            # Use raw connection query to get column information
            def _raw_get_columns(conn, schema, table_name):
                with conn.cursor() as cursor:
                    # Query using system catalogs
                    cursor.execute("""
                        SELECT 
                            a.attname AS column_name,
                            pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type
                        FROM 
                            pg_catalog.pg_attribute a
                        JOIN 
                            pg_catalog.pg_class c ON a.attrelid = c.oid
                        JOIN 
                            pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                        WHERE 
                            n.nspname = %s
                            AND c.relname = %s
                            AND a.attnum > 0
                            AND NOT a.attisdropped
                        ORDER BY
                            a.attnum
                    """, (schema, table_name))
                    columns = []
                    for row in cursor.fetchall():
                        columns.append({"name": row[0], "type": row[1]})
                    return columns
            
            columns = await asyncio.to_thread(_raw_get_columns, self.connection, schema, table_name)
            logger.debug(f"Found {len(columns)} columns for table '{table_name}': {columns}")
            
            if not columns:
                # Fallback to alternative query if needed
                logger.debug(f"No columns found using primary method, trying alternative approach")
                
                def _alt_get_columns(conn, table_name):
                    with conn.cursor() as cursor:
                        cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
                        column_names = [desc[0] for desc in cursor.description]
                        columns = []
                        for name in column_names:
                            # Get type info for each column
                            cursor.execute(f"""
                                SELECT data_type 
                                FROM information_schema.columns 
                                WHERE table_name = %s AND column_name = %s
                            """, (table_name, name))
                            type_result = cursor.fetchone()
                            data_type = type_result[0] if type_result else 'text'
                            columns.append({"name": name, "type": data_type})
                        return columns
                
                alt_columns = await asyncio.to_thread(_alt_get_columns, self.connection, table_name)
                logger.debug(f"Alternative approach found columns: {alt_columns}")
                return alt_columns
            
            return columns
            
        except Exception as e:
            logger.error(f"Error getting columns for table '{table_name}': {str(e)}")
            # Last resort: try information_schema query
            try:
                query = """
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = %s
                    AND table_name = %s;
                """
                result = await self.execute_query(query, (schema, table_name))
                logger.debug(f"Fallback columns found: {result}")
                return [{"name": row['column_name'], "type": row['data_type']} for row in result]
            except Exception as inner_e:
                logger.error(f"Emergency column query also failed: {inner_e}")
                return []
