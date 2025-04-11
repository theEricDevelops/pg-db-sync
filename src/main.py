import asyncio
import os
from typing import Dict

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

async def force_sync(config: Dict):
    """
    Performs a full SQL dump of the source DB, drops the target DB, and imports the dump.
    """
    source_db = DatabaseUtility(config['database']['source_db'])
    target_db = DatabaseUtility(config['database']['target_db'])

    try:
        dump_file = await source_db.dump_database()

        if dump_file:
            logger.info(f"Dump file created: {dump_file}")

            try:
                logger.info("Restoring target database from dump file...")
                await target_db.restore_database(dump_file)
                logger.info("Target database restored successfully.")
            except Exception as e:
                logger.error(f"Error restoring target database: {e}")
                raise HTTPException(status_code=500, detail=f"Error restoring target database: {e}")

            try:
                logger.info("Cleaning up dump file...")
                os.remove(dump_file)
                logger.info(f"Removed dump file: {dump_file}")
            except FileNotFoundError:
                logger.warning(f"Dump file not found for removal: {dump_file}")
            except Exception as e:
                logger.error(f"Error removing dump file: {e}")
        else:
            logger.error("Dump file creation failed.")
            raise HTTPException(status_code=500, detail="Dump file creation failed.")
    except FileNotFoundError:
        logger.error("Dump file not found.")
        raise HTTPException(status_code=500, detail="Dump file not found.")
    except ValueError as ve:
        logger.error(f"Value error occurred: {ve}")
        raise HTTPException(status_code=500, detail=f"Value error: {ve}")
    except PermissionError as pe:
        logger.error(f"Permission error occurred: {pe}")
        raise HTTPException(status_code=500, detail=f"Permission error: {pe}")
    except ConnectionError as ce:
        logger.error(f"Connection error occurred: {ce}")
        raise HTTPException(status_code=500, detail=f"Connection error: {ce}")
    except TimeoutError as te:
        logger.error(f"Timeout error occurred: {te}")
        raise HTTPException(status_code=500, detail=f"Timeout error: {te}")
    except OSError as oe:
        logger.error(f"OS error occurred: {oe}")
        raise HTTPException(status_code=500, detail=f"OS error: {oe}")
    except asyncio.TimeoutError as ate:
        logger.error(f"Asyncio timeout error occurred: {ate}")
        raise HTTPException(status_code=500, detail=f"Asyncio timeout error: {ate}")
    except ImportError as ie:
        logger.error(f"Import error occurred: {ie}")
        raise HTTPException(status_code=500, detail=f"Import error: {ie}")
    except TypeError as te:
        logger.error(f"Type error occurred: {te}")
        raise HTTPException(status_code=500, detail=f"Type error: {te}")
    except AttributeError as ae:
        logger.error(f"Attribute error occurred: {ae}")
        raise HTTPException(status_code=500, detail=f"Attribute error: {ae}")
    except Exception as e:
        logger.error(f"An error occurred during force synchronization: {e}")
        raise HTTPException(status_code=500, detail=f"Force synchronization failed: {e}")
    
    finally:
        try:
            await source_db.disconnect()
            await target_db.disconnect()
        except Exception as e:
            logger.warning(f"Error disconnecting from databases: {e}")

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
    import socket
    import argparse
    import subprocess

    parser = argparse.ArgumentParser(description="PostgreSQL DB Sync Tool")
    parser.add_argument("-f", "--force", action="store_true", help="Perform a full database dump and import.")
    args = parser.parse_args()

    port = 8000
    
    # Check to see if port is available
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(('', port))
            s.close()
            break
        except socket.error:
            port += 1
    
    # Set up logging  
    logger.info("Starting the application...")
    # Load configuration
    config = load_config()

    if args.force:
        logger.info("Force sync option selected. Performing full database dump and import...")
        asyncio.run(force_sync(config))
    else:
        uvicorn.run(app, host="0.0.0.0", port=port)
