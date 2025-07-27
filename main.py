import asyncio
import os
import logging
from pyrogram import Client
from pyrogram.errors import FloodWait
from pymongo import MongoClient
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    filename='upload.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load environment variables
load_dotenv()

# Retrieve environment variables
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
TARGET_CHANNEL = os.getenv("TARGET_CHANNEL")

# Validate environment variables
required_vars = {
    "API_ID": API_ID,
    "API_HASH": API_HASH,
    "BOT_TOKEN": BOT_TOKEN,
    "MONGO_URI": MONGO_URI,
    "DB_NAME": DB_NAME,
    "COLLECTION_NAME": COLLECTION_NAME,
    "TARGET_CHANNEL": TARGET_CHANNEL
}
for var_name, var_value in required_vars.items():
    if not var_value or not isinstance(var_value, str):
        raise ValueError(f"Environment variable {var_name} is missing or not a string. Check your environment settings.")

# Convert API_ID to integer
try:
    API_ID = int(API_ID)
except ValueError:
    raise ValueError("API_ID must be a valid integer.")

# Initialize Pyrogram client
app = Client("bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# MongoDB client
mongo = MongoClient(MONGO_URI)
db = mongo[DB_NAME]
collection = db[COLLECTION_NAME]
sent_collection = db["sent_files"]

async def send_batch(docs):
    """Send a batch of files concurrently, handling errors individually."""
    tasks = []
    for doc in docs:
        file_id = doc.get("_id")
        if file_id:
            tasks.append(
                (file_id, app.send_document(
                    chat_id=TARGET_CHANNEL,
                    document=file_id,
                    caption=doc.get("caption", "")
                ))
            )
    
    results = await asyncio.gather(*[task[1] for task in tasks], return_exceptions=True)
    for (file_id, _), result in zip(tasks, results):
        if isinstance(result, Exception):
            logging.error(f"Failed to send file {file_id}: {result}")
            if isinstance(result, FloodWait):
                return result.value  # Return wait time for FloodWait
        else:
            logging.info(f"Successfully sent document: {file_id}")
            sent_collection.update_one(
                {"_id": file_id},
                {"$set": {"sent": True, "timestamp": result.date}},
                upsert=True
            )
    return 0  # No FloodWait

async def dump_all_files():
    """Send all files from MongoDB collection to the target Telegram channel as documents."""
    async with app:
        total_docs = collection.count_documents({})
        processed = sent_collection.count_documents({})
        batch_size = 10
        cursor = collection.find({"_id": {"$nin": sent_collection.distinct("_id")}}).batch_size(batch_size)
        
        logging.info(f"Starting upload: {total_docs} total documents, {processed} already processed")
        
        batch = []
        for doc in cursor:
            batch.append(doc)
            if len(batch) >= batch_size:
                wait_time = await send_batch(batch)
                processed += len(batch)
                logging.info(f"Processed batch, total {processed}/{total_docs}")
                if wait_time > 0:
                    logging.warning(f"FloodWait triggered, pausing for {wait_time} seconds")
                    await asyncio.sleep(wait_time)
                else:
                    await asyncio.sleep(0.1)  # Minimal delay to avoid rate limits
                batch = []
        
        if batch:
            wait_time = await send_batch(batch)
            processed += len(batch)
            logging.info(f"Processed final batch, total {processed}/{total_docs}")
            if wait_time > 0:
                logging.warning(f"FloodWait triggered, pausing for {wait_time} seconds")
                await asyncio.sleep(wait_time)
        
        logging.info(f"Completed processing {processed}/{total_docs} documents")
        await app.send_message(TARGET_CHANNEL, f"Upload completed: {processed}/{total_docs} documents sent")

if __name__ == "__main__":
    try:
        asyncio.run(dump_all_files())
    except Exception as e:
        logging.error(f"Script terminated with error: {e}")
    finally:
        mongo.close()
