class SyncManager:
    def __init__(self, source_db_connector, target_db_connector, table_mapping):
        self.source_db_connector = source_db_connector
        self.target_db_connector = target_db_connector
        self.table_mapping = table_mapping

    def sync(self):
        for source_table, target_table in self.table_mapping.get_mappings().items():
            self.sync_table(source_table, target_table)

    def sync_table(self, source_table, target_table):
        data = self.fetch_data(source_table)
        self.insert_data(target_table, data)

    def fetch_data(self, table):
        query = f"SELECT * FROM {table};"
        return self.source_db_connector.execute_query(query)

    def insert_data(self, table, data):
        if not data:
            return
        columns = data[0].keys()
        values_placeholder = ', '.join(['%s'] * len(columns))
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({values_placeholder});"
        for row in data:
            self.target_db_connector.execute_query(query, tuple(row.values()))