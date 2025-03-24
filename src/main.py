from db_connector import DatabaseConnector
from sync_manager import SyncManager
from utils.config import load_config
from utils.logger import Logger

def main():
    # Load configuration
    config = load_config()

    # Initialize logger
    logger = Logger()

    # Connect to source and target databases
    source_db = DatabaseConnector(config['source_db'])
    target_db = DatabaseConnector(config['target_db'])

    try:
        source_db.connect()
        target_db.connect()

        # Initialize sync manager
        sync_manager = SyncManager(source_db, target_db)

        # Start synchronization process
        sync_manager.sync()

    except Exception as e:
        logger.error(f"An error occurred: {e}")

    finally:
        source_db.disconnect()
        target_db.disconnect()

if __name__ == "__main__":
    main()