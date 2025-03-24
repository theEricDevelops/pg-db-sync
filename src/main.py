import asyncio
from typing import Dict, List, Tuple

from fastapi import FastAPI, HTTPException
from utils.database import DatabaseUtility
from sync_manager import SyncManager
from utils.config import load_config, get_sync_settings
from utils.logger import Logger

app = FastAPI()
logger = Logger()  # Initialize logger globally

async def sync_data(config: Dict):
    """
    Asynchronously synchronizes data between source and target databases.
    """
    source_db = DatabaseUtility(config['database']['source_db'])
    target_db = DatabaseUtility(config['database']['target_db'])

    try:
        logger.info("Connecting to databases...")
        logger.debug(f"Source DB: {config['database']['source_db']}")
        logger.debug(f"Target DB: {config['database']['target_db']}")
        await source_db.connect()
        await target_db.connect()

        table_mapping = get_sync_settings(config)
        if not table_mapping:
            raise ValueError("Table mapping not found in configuration file")
        logger.debug(f"Table mapping: {table_mapping}")

        sync_manager = SyncManager(source_db, target_db, table_mapping)
        await sync_manager.sync()

    except Exception as e:
        logger.error(f"An error occurred during synchronization: {e}")
        raise HTTPException(status_code=500, detail=f"Synchronization failed: {e}")
    finally:
        await source_db.disconnect()
        await target_db.disconnect()

@app.get("/sync")
async def sync_endpoint():
    """
    Endpoint to trigger data synchronization.
    """
    config = load_config()
    await sync_data(config)
    return {"message": "Data synchronization initiated."}

@app.get("/")
async def root():
    return {"message": "Hello World"}

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting the application...")
    # Load configuration
    config = load_config()
    # Run the sync on startup
    asyncio.run(sync_data(config))
    uvicorn.run(app, host="0.0.0.0", port=8000)
