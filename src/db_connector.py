class DatabaseConnector:
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = None

    def connect(self):
        import psycopg2
        try:
            self.connection = psycopg2.connect(**self.db_config)
            print("Connection to database established.")
        except Exception as e:
            print(f"Error connecting to database: {e}")

    def disconnect(self):
        if self.connection:
            self.connection.close()
            print("Database connection closed.")

    def execute_query(self, query, params=None):
        if not self.connection:
            raise Exception("Database connection is not established.")
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()