from typing import List, Tuple, Dict
from utils.logger import Logger
from utils.database import DatabaseUtility

logger = Logger('sync_manager')

class SyncManager:
    def __init__(self, source_db: DatabaseUtility, target_db: DatabaseUtility, table_mapping: Dict[str, str]):
        self.source_db = source_db
        self.target_db = target_db
        self.table_mapping = table_mapping
        logger.debug(f"SyncManager initialized")

    async def sync(self):
        for source_table, target_table in self.table_mapping.items():
            if source_table == "*":
                logger.debug(f"Syncing all tables")
                all_tables = await self.source_db.fetch_all_tables()
                logger.debug(f"Tables found: {all_tables}")
                for table in all_tables:
                    await self.sync_table(table)
            else:
                logger.debug(f"Syncing table: {source_table}")
    
    async def sync_table(self, source_table: str, target_table: str = None):
        if not target_table:
            target_table = source_table
        
        try: 
            source_columns = await self.source_db.get_table_columns(source_table)
        except Exception as e:
            logger.error(f"Error fetching columns: {e}")
        
        try:
            await self.target_db.create_table(target_table, source_columns)
        except Exception as e:
            logger.error(f"Error creating table: {e}")

        try:
            source_data =await self.source_db.fetch_table_data(source_table)
        except Exception as e:
            logger.error(f"Error fetching data: {e}")

        try:
            await self.target_db.insert_data(target_table, source_data)
        except Exception as e:
            logger.error(f"Error inserting data: {e}")
