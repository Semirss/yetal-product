import logging
import re
import os
import json
from datetime import datetime
from uuid import uuid4
import pandas as pd
import tempfile
import asyncio
from flask import Flask
import threading
import os
import boto3
import io
import hashlib
import time
import requests
import redis
from telegram.ext import BasePersistence
# Import scraper module
import scraper
import shutil

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    InputMediaPhoto,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    filters,
    PicklePersistence,
)
from telegram.error import BadRequest
from telegram.request import HTTPXRequest
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables
load_dotenv()

# Load MongoDB configuration
logger = logging.getLogger(__name__)
Main_channelusername =os.getenv("Main_channelusername")
MONGO_URI = os.getenv("MONGO_URI")
if MONGO_URI:
    client = MongoClient(MONGO_URI)
    db = client["yetal"]
    channels_collection = db["yetalcollection"]
    categories_collection = db["categories"]
    logger.info("✅ MongoDB connected successfully")
else:
    logger.warning("⚠️ MONGO_URI not set, MongoDB features will be disabled")
    channels_collection = None
    categories_collection = None

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)

# Load configuration
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
MASTER_DATA_CHANNEL_ID = os.getenv("MASTER_DATA_CHANNEL_ID")
PRODUCT_CHANNEL_ID = os.getenv("PRODUCT_CHANNEL_ID")
YETAL_SEARCH_USERNAME = os.getenv("YETAL_SEARCH_USERNAME", "Yetal_Search")

# S3 configuration - FIXED to work without ListAllMyBuckets permission
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME") or os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")

# Redis configuration for session persistence (using custom RedisPersistence class)
REDIS_URL = os.getenv("REDIS_URL")

# Enhanced debugging
logger.info(f"🔧 S3 Configuration Debug:")
logger.info(f"   - AWS_BUCKET_NAME: {AWS_BUCKET_NAME}")
logger.info(f"   - AWS_ACCESS_KEY_ID: {'SET' if AWS_ACCESS_KEY_ID and len(AWS_ACCESS_KEY_ID) > 10 else 'INVALID/EMPTY'}")
logger.info(f"   - AWS_SECRET_ACCESS_KEY: {'SET' if AWS_SECRET_ACCESS_KEY and len(AWS_SECRET_ACCESS_KEY) > 10 else 'INVALID/EMPTY'}")
logger.info(f"   - AWS_REGION: {AWS_REGION}")

# FIXED S3 client initialization - works without ListAllMyBuckets permission
def get_s3_client():
    """Initialize S3 client that works without ListAllMyBuckets permission"""
    try:
        # Check if we have the minimum required configuration
        if not AWS_BUCKET_NAME:
            logger.error("❌ AWS_BUCKET_NAME is not set!")
            return None
            
        if not AWS_ACCESS_KEY_ID or len(AWS_ACCESS_KEY_ID) < 10:
            logger.error("❌ AWS_ACCESS_KEY_ID is invalid or too short")
            return None
            
        if not AWS_SECRET_ACCESS_KEY or len(AWS_SECRET_ACCESS_KEY) < 10:
            logger.error("❌ AWS_SECRET_ACCESS_KEY is invalid or too short")
            return None

        logger.info("🔧 Attempting to create S3 client...")
        
        # Create session with explicit credentials
        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID.strip(),
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY.strip(),
            region_name=AWS_REGION.strip()
        )
        
        s3_client = session.client('s3')
        
        # Test the connection WITHOUT ListAllMyBuckets - use head_bucket instead
        logger.info("🔧 Testing S3 connection with head_bucket (no ListAllMyBuckets needed)...")
        try:
            s3_client.head_bucket(Bucket=AWS_BUCKET_NAME)
            logger.info("✅ S3 client authenticated successfully")
            return s3_client
        except s3_client.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"❌ Bucket '{AWS_BUCKET_NAME}' not found")
                return None
            elif error_code == '403':
                logger.error(f"❌ Access denied to bucket '{AWS_BUCKET_NAME}' - check permissions")
                # But we can still try to use the client for operations that might work
                logger.info("⚠️ Continuing with S3 client despite access issues - some operations may fail")
                return s3_client
            else:
                logger.error(f"❌ S3 connection test failed: {error_code}")
                return None
            
    except Exception as e:
        logger.error(f"❌ S3 client initialization failed: {str(e)}")
        return None

# Initialize S3 client
S3 = get_s3_client()

# Test S3 connection if configured
if S3 and AWS_BUCKET_NAME:
    logger.info(f"✅ S3 client created successfully")
    logger.info(f"✅ S3 bucket: {AWS_BUCKET_NAME}")
    
    # Test bucket access with better error handling
    try:
        # Test with head_bucket instead of list_buckets
        try:
            S3.head_bucket(Bucket=AWS_BUCKET_NAME)
            logger.info("✅ S3 bucket access verified")
            
            # Test write permissions by creating a test object
            try:
                test_key = "test_write_permission.txt"
                S3.put_object(
                    Bucket=AWS_BUCKET_NAME,
                    Key=test_key,
                    Body=b"Test write permission",
                    ContentType='text/plain'
                )
                S3.delete_object(Bucket=AWS_BUCKET_NAME, Key=test_key)
                logger.info("✅ S3 write permissions verified")
            except Exception as e:
                logger.warning(f"⚠️ S3 write permissions limited: {e}")
                
        except Exception as e:
            error_code = e.response['Error']['Code'] if hasattr(e, 'response') else 'Unknown'
            logger.warning(f"⚠️ S3 bucket access limited ({error_code}): {e}")
            # Don't set S3 to None - we'll try to use it anyway for operations that might work
            
    except Exception as e:
        logger.warning(f"⚠️ S3 credentials test had issues: {e}")
        # Continue with S3 client - some operations might still work
else: 
    if not S3:
        logger.error("❌ S3 client creation failed")
    if not AWS_BUCKET_NAME:
        logger.error("❌ AWS_BUCKET_NAME is not configured")
    logger.warning("⚠️ S3 functionality will be disabled")

HUGGINGFACE_API_TOKEN = os.getenv("HUGGINGFACE_API_TOKEN")
logger.info(f"🔧 Hugging Face API Token: {'SET' if HUGGINGFACE_API_TOKEN and len(HUGGINGFACE_API_TOKEN) > 10 else 'INVALID/EMPTY'}")

# Custom Redis Persistence Class with S3 Backup
class RedisPersistence(BasePersistence):
    """Custom Redis persistence for telegram bot session data with S3 backup"""

    def __init__(self, url=None, host='localhost', port=6379, db=0, password=None, prefix='telegram_bot'):
        super().__init__()
        self.prefix = prefix
        
        # Connect to Redis with retry_on_timeout and socket_keepalive
        if url:
            self.redis = redis.from_url(
                url,
                retry_on_timeout=True,
                socket_keepalive=True,
                decode_responses=True
            )
        else:
            self.redis = redis.Redis(
                host=host, 
                port=port, 
                db=db, 
                password=password, 
                decode_responses=True,
                retry_on_timeout=True,
                socket_keepalive=True
            )
        
        # Initialize S3 client for backup
        self.s3_client = S3 if S3 else None
        self.bucket_name = AWS_BUCKET_NAME
        self.s3_backup_key = "redis_backups/user_data_backup.json"
        self._auto_backup_task = None
        
        # Check if Redis is empty on startup and restore from S3 if needed
        self._check_and_restore_on_startup()

    def _check_and_restore_on_startup(self):
        """Check if Redis is empty and restore from S3 if needed"""
        try:
            # Check if any user data keys exist
            pattern = f"{self.prefix}:user_data:*"
            keys = self.redis.keys(pattern)
            
            if not keys or len(keys) == 0:
                logger.info("ℹ️ Redis appears to be empty, attempting to restore from S3...")
                try:
                    self.restore_from_s3()
                    logger.info("✅ Successfully restored data from S3 on startup")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to restore from S3 on startup: {e}")
            else:
                logger.info(f"✅ Redis contains {len(keys)} user data keys, skipping restore")
        except Exception as e:
            logger.error(f"❌ Error checking Redis on startup: {e}")

    def _get_user_key(self, user_id):
        """Generate Redis key for a user"""
        return f"{self.prefix}:user_data:{user_id}"

    async def get_user_data(self):
        """Get all user data from Redis (stored in individual keys)"""
        try:
            pattern = f"{self.prefix}:user_data:*"
            keys = self.redis.keys(pattern)
            all_data = {}
            for key in keys:
                try:
                    user_id = int(key.split(":")[-1])
                    data = self.redis.get(key)
                    if data:
                        all_data[user_id] = json.loads(data)
                except (ValueError, json.JSONDecodeError) as e:
                    logger.warning(f"Error parsing user data from key {key}: {e}")
                    continue
            return all_data
        except Exception as e:
            logger.error(f"Error getting user data from Redis: {e}")
            return {}

    async def get_chat_data(self):
        """Get chat data from Redis"""
        try:
            data = self.redis.get(f"{self.prefix}:chat_data")
            return json.loads(data) if data else {}
        except Exception as e:
            logger.error(f"Error getting chat data from Redis: {e}")
            return {}

    async def get_bot_data(self):
        """Get bot data from Redis"""
        try:
            data = self.redis.get(f"{self.prefix}:bot_data")
            return json.loads(data) if data else {}
        except Exception as e:
            logger.error(f"Error getting bot data from Redis: {e}")
            return {}

    async def get_conversations(self, name):
        """Get conversations from Redis"""
        try:
            data = self.redis.get(f"{self.prefix}:conversations:{name}")
            return json.loads(data) if data else {}
        except Exception as e:
            logger.error(f"Error getting conversations from Redis: {e}")
            return {}

    async def get_callback_data(self):
        """Get callback data from Redis"""
        try:
            data = self.redis.get(f"{self.prefix}:callback_data")
            return json.loads(data) if data else {}
        except Exception as e:
            logger.error(f"Error getting callback data from Redis: {e}")
            return {}

    async def update_user_data(self, user_id, data):
        """Update user data in Redis (stored in individual key per user)"""
        try:
            key = self._get_user_key(user_id)
            self.redis.set(key, json.dumps(data))
        except Exception as e:
            logger.error(f"Error updating user data in Redis: {e}")

    async def update_chat_data(self, chat_id, data):
        """Update chat data in Redis"""
        try:
            all_data = await self.get_chat_data()
            all_data[chat_id] = data
            self.redis.set(f"{self.prefix}:chat_data", json.dumps(all_data))
        except Exception as e:
            logger.error(f"Error updating chat data in Redis: {e}")

    async def update_bot_data(self, data):
        """Update bot data in Redis"""
        try:
            self.redis.set(f"{self.prefix}:bot_data", json.dumps(data))
        except Exception as e:
            logger.error(f"Error updating bot data in Redis: {e}")

    async def update_conversation(self, name, key, new_state):
        """Update conversation in Redis"""
        try:
            all_data = await self.get_conversations(name)
            # Convert tuple keys to strings for JSON serialization
            if isinstance(key, tuple):
                key = str(key)
            all_data[key] = new_state
            self.redis.set(f"{self.prefix}:conversations:{name}", json.dumps(all_data))
        except Exception as e:
            logger.error(f"Error updating conversation in Redis: {e}")

    async def update_callback_data(self, data):
        """Update callback data in Redis"""
        try:
            self.redis.set(f"{self.prefix}:callback_data", json.dumps(data))
        except Exception as e:
            logger.error(f"Error updating callback data in Redis: {e}")

    async def drop_user_data(self, user_id):
        """Drop user data from Redis"""
        try:
            key = self._get_user_key(user_id)
            self.redis.delete(key)
        except Exception as e:
            logger.error(f"Error dropping user data from Redis: {e}")

    async def drop_chat_data(self, chat_id):
        """Drop chat data from Redis"""
        try:
            all_data = await self.get_chat_data()
            if chat_id in all_data:
                del all_data[chat_id]
                self.redis.set(f"{self.prefix}:chat_data", json.dumps(all_data))
        except Exception as e:
            logger.error(f"Error dropping chat data from Redis: {e}")

    async def refresh_user_data(self, user_id, user_data):
        """Refresh user data in Redis"""
        try:
            await self.update_user_data(user_id, user_data)
        except Exception as e:
            logger.error(f"Error refreshing user data in Redis: {e}")

    async def refresh_chat_data(self, chat_id, chat_data):
        """Refresh chat data in Redis"""
        try:
            await self.update_chat_data(chat_id, chat_data)
        except Exception as e:
            logger.error(f"Error refreshing chat data in Redis: {e}")

    async def refresh_bot_data(self, bot_data):
        """Refresh bot data in Redis"""
        try:
            await self.update_bot_data(bot_data)
        except Exception as e:
            logger.error(f"Error refreshing bot data in Redis: {e}")

    async def flush(self):
        """Flush all data to Redis (no-op since we update immediately)"""
        pass

    def backup_to_s3(self):
        """
        Backup all user data keys to S3
        
        Returns:
            bool: True if backup was successful, False otherwise
        """
        if not self.s3_client or not self.bucket_name:
            logger.error("❌ S3 client or bucket name not configured for backup")
            return False
        
        try:
            # Get all user data keys
            pattern = f"{self.prefix}:user_data:*"
            keys = self.redis.keys(pattern)
            
            if not keys:
                logger.info("ℹ️ No user data keys found to backup")
                return True
            
            # Collect all user data
            backup_data = {}
            for key in keys:
                try:
                    # Extract user_id from key (format: prefix:user_data:user_id)
                    user_id = key.split(":")[-1]
                    data = self.redis.get(key)
                    if data:
                        backup_data[user_id] = json.loads(data)
                except Exception as e:
                    logger.warning(f"⚠️ Error reading key {key} for backup: {e}")
                    continue
            
            if not backup_data:
                logger.warning("⚠️ No data to backup after processing keys")
                return False
            
            # Serialize backup data
            backup_json = json.dumps(backup_data, indent=2)
            
            # Upload to S3
            try:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=self.s3_backup_key,
                    Body=backup_json.encode('utf-8'),
                    ContentType='application/json'
                )
                logger.info(f"✅ Successfully backed up {len(backup_data)} user records to S3")
                return True
            except Exception as e:
                logger.error(f"❌ Failed to upload backup to S3: {e}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error during backup: {e}")
            return False

    def restore_from_s3(self):
        """
        Restore all keys from S3 backup
        
        Returns:
            bool: True if restore was successful, False otherwise
        """
        if not self.s3_client or not self.bucket_name:
            logger.error("❌ S3 client or bucket name not configured for restore")
            return False
        
        try:
            # Check if backup file exists
            try:
                self.s3_client.head_object(Bucket=self.bucket_name, Key=self.s3_backup_key)
            except self.s3_client.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    logger.warning(f"⚠️ Backup file {self.s3_backup_key} not found in S3")
                    return False
                else:
                    raise
            
            # Download backup from S3
            try:
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.s3_backup_key)
                backup_json = response['Body'].read().decode('utf-8')
                backup_data = json.loads(backup_json)
            except Exception as e:
                logger.error(f"❌ Failed to download or parse backup from S3: {e}")
                return False
            
            if not backup_data:
                logger.warning("⚠️ Backup file is empty")
                return False
            
            # Restore data to Redis (using individual keys)
            restored_count = 0
            for user_id, data in backup_data.items():
                try:
                    key = self._get_user_key(user_id)
                    self.redis.set(key, json.dumps(data))
                    restored_count += 1
                except Exception as e:
                    logger.warning(f"⚠️ Error restoring data for user {user_id}: {e}")
                    continue
            
            logger.info(f"✅ Successfully restored {restored_count} user records from S3")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error during restore: {e}")
            return False

    async def auto_backup(self):
        """
        Async coroutine that runs automatic backups every 20 minutes
        
        This should be started as a background task
        """
        while True:
            try:
                await asyncio.sleep(20 * 60)  # Wait 20 minutes
                logger.info("🔄 Starting automatic backup...")
                success = self.backup_to_s3()
                if success:
                    logger.info("✅ Automatic backup completed successfully")
                else:
                    logger.warning("⚠️ Automatic backup failed")
            except asyncio.CancelledError:
                logger.info("🛑 Auto-backup task cancelled")
                break
            except Exception as e:
                logger.error(f"❌ Error in auto-backup loop: {e}")
                # Continue the loop even if backup fails
                await asyncio.sleep(60)  # Wait a minute before retrying

# Redis Persistence Manager with S3 Backup
class RedisPersistenceManager:
    """Manages Redis persistence with S3 backup/restore functionality for user data"""
    
    def __init__(self, redis_url=None, prefix='telegram_bot'):
        """
        Initialize Redis persistence manager
        
        Args:
            redis_url: Redis connection URL (from REDIS_URL env var)
            prefix: Key prefix for user data (default: 'telegram_bot')
        """
        self.prefix = prefix
        self.redis_url = redis_url or REDIS_URL
        
        if not self.redis_url:
            raise ValueError("REDIS_URL must be provided or set in environment variables")
        
        # Connect to Redis with specified parameters
        self.redis = redis.from_url(
            self.redis_url,
            retry_on_timeout=True,
            socket_keepalive=True,
            decode_responses=True
        )
        
        # Test connection
        try:
            self.redis.ping()
            logger.info("✅ Redis connection established")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Redis: {e}")
            raise
        
        # Initialize S3 client if available
        self.s3_client = S3 if S3 else None
        self.bucket_name = AWS_BUCKET_NAME
        self.s3_backup_key = "redis_backups/user_data_backup.json"
        
        # Check if Redis is empty on startup and restore if needed
        self._check_and_restore_on_startup()
        
        # Start auto-backup coroutine in background
        self._auto_backup_task = None
    
    def _check_and_restore_on_startup(self):
        """Check if Redis is empty and restore from S3 if needed"""
        try:
            # Check if any user data keys exist
            pattern = f"{self.prefix}:user_data:*"
            keys = self.redis.keys(pattern)
            
            if not keys or len(keys) == 0:
                logger.info("ℹ️ Redis appears to be empty, attempting to restore from S3...")
                try:
                    self.restore_from_s3()
                    logger.info("✅ Successfully restored data from S3 on startup")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to restore from S3 on startup: {e}")
            else:
                logger.info(f"✅ Redis contains {len(keys)} user data keys, skipping restore")
        except Exception as e:
            logger.error(f"❌ Error checking Redis on startup: {e}")
    
    def get_user_key(self, user_id):
        """Generate Redis key for a user"""
        return f"{self.prefix}:user_data:{user_id}"
    
    def get_user_data(self, user_id):
        """Get user data from Redis"""
        try:
            key = self.get_user_key(user_id)
            data = self.redis.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Error getting user data for {user_id}: {e}")
            return None
    
    def set_user_data(self, user_id, data):
        """Store user data in Redis"""
        try:
            key = self.get_user_key(user_id)
            self.redis.set(key, json.dumps(data))
            return True
        except Exception as e:
            logger.error(f"Error setting user data for {user_id}: {e}")
            return False
    
    def delete_user_data(self, user_id):
        """Delete user data from Redis"""
        try:
            key = self.get_user_key(user_id)
            self.redis.delete(key)
            return True
        except Exception as e:
            logger.error(f"Error deleting user data for {user_id}: {e}")
            return False
    
    def get_all_user_keys(self):
        """Get all user data keys from Redis"""
        try:
            pattern = f"{self.prefix}:user_data:*"
            keys = self.redis.keys(pattern)
            return keys
        except Exception as e:
            logger.error(f"Error getting all user keys: {e}")
            return []
    
    def backup_to_s3(self):
        """
        Backup all user data keys to S3
        
        Returns:
            bool: True if backup was successful, False otherwise
        """
        if not self.s3_client or not self.bucket_name:
            logger.error("❌ S3 client or bucket name not configured for backup")
            return False
        
        try:
            # Get all user data keys
            pattern = f"{self.prefix}:user_data:*"
            keys = self.redis.keys(pattern)
            
            if not keys:
                logger.info("ℹ️ No user data keys found to backup")
                return True
            
            # Collect all user data
            backup_data = {}
            for key in keys:
                try:
                    # Extract user_id from key (format: prefix:user_data:user_id)
                    user_id = key.split(":")[-1]
                    data = self.redis.get(key)
                    if data:
                        backup_data[user_id] = json.loads(data)
                except Exception as e:
                    logger.warning(f"⚠️ Error reading key {key} for backup: {e}")
                    continue
            
            if not backup_data:
                logger.warning("⚠️ No data to backup after processing keys")
                return False
            
            # Serialize backup data
            backup_json = json.dumps(backup_data, indent=2)
            
            # Upload to S3
            try:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=self.s3_backup_key,
                    Body=backup_json.encode('utf-8'),
                    ContentType='application/json'
                )
                logger.info(f"✅ Successfully backed up {len(backup_data)} user records to S3")
                return True
            except Exception as e:
                logger.error(f"❌ Failed to upload backup to S3: {e}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error during backup: {e}")
            return False
    
    def restore_from_s3(self):
        """
        Restore all keys from S3 backup
        
        Returns:
            bool: True if restore was successful, False otherwise
        """
        if not self.s3_client or not self.bucket_name:
            logger.error("❌ S3 client or bucket name not configured for restore")
            return False
        
        try:
            # Check if backup file exists
            try:
                self.s3_client.head_object(Bucket=self.bucket_name, Key=self.s3_backup_key)
            except self.s3_client.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    logger.warning(f"⚠️ Backup file {self.s3_backup_key} not found in S3")
                    return False
                else:
                    raise
            
            # Download backup from S3
            try:
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.s3_backup_key)
                backup_json = response['Body'].read().decode('utf-8')
                backup_data = json.loads(backup_json)
            except Exception as e:
                logger.error(f"❌ Failed to download or parse backup from S3: {e}")
                return False
            
            if not backup_data:
                logger.warning("⚠️ Backup file is empty")
                return False
            
            # Restore data to Redis
            restored_count = 0
            for user_id, data in backup_data.items():
                try:
                    key = self.get_user_key(user_id)
                    self.redis.set(key, json.dumps(data))
                    restored_count += 1
                except Exception as e:
                    logger.warning(f"⚠️ Error restoring data for user {user_id}: {e}")
                    continue
            
            logger.info(f"✅ Successfully restored {restored_count} user records from S3")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error during restore: {e}")
            return False
    
    async def auto_backup(self):
        """
        Async coroutine that runs automatic backups every 20 minutes
        
        This should be started as a background task
        """
        while True:
            try:
                await asyncio.sleep(20 * 60)  # Wait 20 minutes
                logger.info("🔄 Starting automatic backup...")
                success = self.backup_to_s3()
                if success:
                    logger.info("✅ Automatic backup completed successfully")
                else:
                    logger.warning("⚠️ Automatic backup failed")
            except asyncio.CancelledError:
                logger.info("🛑 Auto-backup task cancelled")
                break
            except Exception as e:
                logger.error(f"❌ Error in auto-backup loop: {e}")
                # Continue the loop even if backup fails
                await asyncio.sleep(60)  # Wait a minute before retrying
    
    def start_auto_backup(self):
        """Start the auto-backup coroutine in a background task"""
        if self._auto_backup_task is None or self._auto_backup_task.done():
            try:
                loop = asyncio.get_event_loop()
                self._auto_backup_task = loop.create_task(self.auto_backup())
                logger.info("✅ Auto-backup task started (runs every 20 minutes)")
            except RuntimeError:
                # No event loop running, will need to start it when available
                logger.warning("⚠️ No event loop available, auto-backup will start when loop is available")
    
    def stop_auto_backup(self):
        """Stop the auto-backup coroutine"""
        if self._auto_backup_task and not self._auto_backup_task.done():
            self._auto_backup_task.cancel()
            logger.info("🛑 Auto-backup task stopped")

# Helper function for Hugging Face API calls
def call_huggingface_api(model: str, payload: dict) -> dict:
    """Call Hugging Face Inference API with error handling"""
    headers = {
        "Authorization": f"Bearer {HUGGINGFACE_API_TOKEN}",
        "Content-Type": "application/json"
    }
    api_url = f"https://api-inference.huggingface.co/models/{model}"
    try:
        response = requests.post(api_url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"❌ Hugging Face API call failed for {model}: {e}")
        return None

# Parse admin IDs from environment variable
ADMIN_IDS = []
admin_ids_str = os.getenv("ADMIN_IDS", "")
if admin_ids_str:
    try:
        ADMIN_IDS = [
            int(uid.strip()) for uid in admin_ids_str.split(",") if uid.strip()
        ]
    except Exception:
        logger.warning("Invalid ADMIN_IDS format. Should be comma-separated integers.")

if not BOT_TOKEN or not MASTER_DATA_CHANNEL_ID or not PRODUCT_CHANNEL_ID:
    logger.critical(
        "Missing required environment variables (TELEGRAM_BOT_TOKEN, MASTER_DATA_CHANNEL_ID, PRODUCT_CHANNEL_ID)"
    )
    exit(1)

# Conversation states
(
    AWAITING_IMAGE,
    AWAITING_TITLE,
    AWAITING_DESCRIPTION,
    AWAITING_PRICE,
    AWAITING_PRICE_VISIBILITY,
    AWAITING_STOCK,
    AWAITING_CHANNEL_USERNAME,
    AWAITING_CHANNEL_CONTACT,
    AWAITING_CHANNEL_LOCATION,
    AWAITING_CHANNEL_CONFIRMATION,
    EDIT_SELECT_PRODUCT,
    EDIT_FIELD_SELECT,
    EDIT_NEW_VALUE,
    REPOST_SELECT_DAYS,
    REPOST_GET_CHANNEL,
    REPOST_CONFIRM
) = range(17)

# Constants
MAX_TITLE_LENGTH = 100
MAX_DESCRIPTION_LENGTH = 500
IMAGE_FOLDER = "product_images"

# --- Keep Alive Configuration ---
KEEP_ALIVE_INTERVAL = 300  # 5 minutes in seconds
HEALTH_CHECK_URL = os.getenv("RENDER_EXTERNAL_URL") or os.getenv("RAILWAY_STATIC_URL") or "http://localhost:10000"

# === 🔄 JSON File Management (S3) ===

def load_json_from_s3(s3_key):
    """Load JSON data directly from S3"""
    try:
        response = S3.get_object(Bucket=AWS_BUCKET_NAME, Key=s3_key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        logger.info(f"✅ Loaded JSON from S3: {s3_key}")
        return data
    except S3.exceptions.NoSuchKey:
        logger.info(f"ℹ️ JSON file {s3_key} not found in S3, returning empty list")
        return []
    except Exception as e:
        logger.error(f"❌ Error loading JSON from S3: {e}")
        return []

def save_json_to_s3(data, s3_key):
    """Save JSON data directly to S3"""
    try:
        S3.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(data, indent=2).encode('utf-8'),
            ContentType='application/json'
        )
        logger.info(f"✅ Saved JSON to S3: {s3_key} ({len(data)} records)")
        return True
    except Exception as e:
        logger.error(f"❌ Error saving JSON to S3: {e}")
        return False

def append_to_scraped_data(new_product_data):
    """Append new product data to scraped_data.json in S3 with correct format"""
    try:
        s3_key = "data/scraped_data.json"

        # Load existing data
        existing_data = load_json_from_s3(s3_key)

        # Enhanced duplicate detection
        def is_duplicate(existing, new):
            """Check if products are duplicates based on multiple criteria"""
            # Check product_ref first
            if existing.get('product_ref') and new.get('product_ref'):
                if existing['product_ref'] == new['product_ref']:
                    return True

            # Check title + price + channel combination
            if (existing.get('title') == new.get('title') and
                existing.get('price') == new.get('price') and
                existing.get('channel') == new.get('channel')):
                return True

            return False

        # Remove duplicates based on product_ref
        filtered_data = [item for item in existing_data
                        if not is_duplicate(item, new_product_data)]

        # Add new product with correct field names
        filtered_data.append(new_product_data)

        # Save back to S3
        success = save_json_to_s3(filtered_data, s3_key)

        if success:
            logger.info(f"✅ Appended product to scraped_data.json. Total records: {len(filtered_data)}")
            return True
        else:
            logger.error("❌ Failed to save updated data to S3")
            return False

    except Exception as e:
        logger.error(f"❌ Error appending to scraped data: {e}")
        return False

def update_scraped_data(updated_product_data):
    """Update existing product data in scraped_data.json in S3 by product_ref"""
    try:
        s3_key = "data/scraped_data.json"

        # Load existing data
        existing_data = load_json_from_s3(s3_key)

        if not existing_data:
            logger.warning("⚠️ No existing data found, cannot update")
            return False

        # Find and update the existing record by product_ref
        product_ref = updated_product_data.get('product_ref')
        if not product_ref:
            logger.error("❌ No product_ref provided for update")
            return False

        updated = False
        for i, item in enumerate(existing_data):
            if item.get('product_ref') == product_ref:
                # Update the existing record with new data
                existing_data[i] = updated_product_data
                updated = True
                logger.info(f"✅ Updated existing record for product_ref: {product_ref}")
                break

        if not updated:
            logger.warning(f"⚠️ Product with product_ref {product_ref} not found, cannot update")
            return False

        # Save back to S3
        success = save_json_to_s3(existing_data, s3_key)

        if success:
            logger.info(f"✅ Updated product in scraped_data.json. Total records: {len(existing_data)}")
            return True
        else:
            logger.error("❌ Failed to save updated data to S3")
            return False

    except Exception as e:
        logger.error(f"❌ Error updating scraped data: {e}")
        return False

# --- Helper Functions ---

def is_admin(user_id: int) -> bool:
    """Checks if a user is in the admin list."""
    # ⚠️ ACCESS CONTROL DISABLED: All users are admins
    return True
    # return user_id in ADMIN_IDS

def get_admin_inline_keyboard() -> InlineKeyboardMarkup:
    """Generates an inline keyboard with an admin button."""
    keyboard = [
        [InlineKeyboardButton("📊 View JSON Data", callback_data="view_json_data")]
    ]
    return InlineKeyboardMarkup(keyboard)

def validate_phone(phone: str) -> bool:
    """Validates phone number format"""
    return bool(re.match(r"^[\d\s\+\-\(\)]{7,20}$", phone))

async def save_image_file(photo_file, user_id: int) -> str:
    """Saves product image to disk"""
    os.makedirs(IMAGE_FOLDER, exist_ok=True)
    file_path = f"{IMAGE_FOLDER}/{user_id}_{uuid4()}.jpg"
    await photo_file.download_to_drive(file_path)
    return file_path

async def _send_master_record(bot, record_type: str, data: dict, message_id: str = None) -> Message | None:
    """Sends a JSON record to the Master Data Channel."""
    # Prepare base record with type and date
    record = {"type": record_type, "date": datetime.now().isoformat()}

    # Add fields based on record type
    if record_type == "channel_ref":
        record.update(
            {
                "channel": data.get("username", "N/A"),
                "phone": data.get("contact", "Not provided"),
                "location": data.get("location", "Not provided"),
            }
        )
    else:
        record.update(
            {
                "title": data.get("title", "N/A"),
                "description": data.get("description", "No description"),
                "price": str(data.get("price", 0)),
                "phone": data.get("contact", "Not provided"),
                "location": data.get("location", "Not provided"),
                "image": data.get("photo_file_id", "No image"),
                "channel": data.get("username", "N/A"),
                "product_ref": message_id if message_id else str(uuid4()),  # Use actual message ID
                # ✅ ADD AI FIELDS TO MASTER RECORD
                "predicted_category": data.get("predicted_category", "Other"),
                "generated_description": data.get("generated_description", data.get("description", "No description"))
            }
        )

    try:
        sent = await bot.send_message(
            chat_id=MASTER_DATA_CHANNEL_ID,
            text=json.dumps(record, indent=2),
            disable_notification=True,
            parse_mode=None,
        )
        return sent
    except Exception as e:
        logger.error(f"Failed to send master record: {e}")
        return None

def format_product_message(product_data: dict, channel_data: dict, post_link: str = None, is_edited: bool = False) -> str:
    """Formats product data for display in channels"""
    message = (
        f"*{product_data.get('title', 'N/A')}*\n\n"
        f"{product_data.get('description', 'No description')}\n\n"
    )

    if product_data.get("price_visible", 1):
        message += f"💰 Price: ETB {float(product_data.get('price', 0)):.2f}\n"
    else:
        message += "💰 Price: Contact for price\n"

    if product_data.get("stock", -1) >= 0:
        message += f"📦 Stock: {product_data['stock']}\n"

    message += (
        f"📍 Location: {channel_data.get('location', 'Not provided')}\n"
        f"📞 Contact: {channel_data.get('contact', 'Not provided')}\n"
    )

    # Add post link if provided
    if post_link:
        message += f"🔗 [View Original Post]({post_link})\n"

    # Add edited indicator at the end if this is an edited post
    if is_edited:
        message += "**edited**\n"

    return message

async def post_product_to_channels(
    product_data: dict, channel_data: dict, context: ContextTypes.DEFAULT_TYPE, is_edited: bool = False
) -> tuple[bool, str | None, int | None, int | None, int | None, str | None]:
    """Posts product to user channel and product channel, sends JSON to master channel
    Returns: (success, target_channel_id, user_message_id, master_message_id, yetal_message_id, target_channel_username)
    """
    user_id = context.user_data.get("user_id")
    target_channel_id = channel_data.get("channel_id")

    # Post to user channel first to get the message ID for the post link
    user_msg_id = None
    target_channel_username = None
    yetal_msg_id = None  # This will store the Yetal_Search channel message ID
    user_message = ""
    
    # Get media paths if available (for scraped items)
    media_paths = product_data.get("media_paths", [])
    
    try:
        # Post to user channel if available
        if target_channel_id:
            user_message = format_product_message(product_data, channel_data)
            
            # Case 1: Media Group (Album)
            if media_paths and len(media_paths) > 1:
                media_group = []
                for idx, path in enumerate(media_paths):
                    try:
                        # Attach caption only to the first photo
                        caption = user_message if idx == 0 else None
                        media_group.append(InputMediaPhoto(open(path, 'rb'), caption=caption, parse_mode="Markdown"))
                    except Exception as e:
                        logger.error(f"Error opening media path {path}: {e}")
                
                if media_group:
                    msgs = await context.bot.send_media_group(chat_id=target_channel_id, media=media_group)
                    user_msg_id = msgs[0].message_id
            
            # Case 2: Single File ID (from /addproduct)
            elif product_data.get("photo_file_id"):
                user_msg = await context.bot.send_photo(
                    chat_id=target_channel_id,
                    photo=product_data["photo_file_id"],
                    caption=user_message,
                    parse_mode="Markdown",
                )
                user_msg_id = user_msg.message_id
                
            # Case 3: Single Local File (from scraped single item)
            elif media_paths and len(media_paths) == 1:
                user_msg = await context.bot.send_photo(
                    chat_id=target_channel_id,
                    photo=open(media_paths[0], 'rb'),
                    caption=user_message,
                    parse_mode="Markdown",
                )
                user_msg_id = user_msg.message_id
                
            # Case 4: Text Only
            else:
                user_msg = await context.bot.send_message(
                    chat_id=target_channel_id, text=user_message, parse_mode="Markdown"
                )
                user_msg_id = user_msg.message_id
            
            target_channel_username = channel_data.get("username", "").replace("@", "")
            logger.info(f"✅ Product posted to user channel: {target_channel_id}, Message ID: {user_msg_id}")

        # Send JSON to master channel and capture its message id
        master_msg = await _send_master_record(
            context.bot,
            "product_ref",
            {
                "title": product_data.get("title"),
                "description": product_data.get("description"),
                "price": product_data.get("price"),
                "contact": channel_data.get("contact"),
                "location": channel_data.get("location"),
                "photo_file_id": product_data.get("photo_file_id"),
                "username": channel_data.get("username"),
                "predicted_category": product_data.get("predicted_category"),
                "generated_description": product_data.get("generated_description"),
                "has_media_group": len(media_paths) > 1 
            },
            message_id=str(user_msg_id) if user_msg_id else None
        )
        master_message_id = master_msg.message_id if master_msg else None

        # Post to Yetal_Search product channel (this is the important one for post links)
        if PRODUCT_CHANNEL_ID:
            try:
                # Create post link using user channel message ID
                if user_msg_id and target_channel_username:
                    post_link = f"https://t.me/{target_channel_username}/{user_msg_id}"
                else:
                    post_link = None
                
                # Format message with actual post link
                master_message = format_product_message(
                    {**product_data, "price_visible": 1},
                    channel_data,
                    post_link,
                    is_edited
                )
                
                # Case 1: Media Group (Album)
                if media_paths and len(media_paths) > 1:
                    media_group = []
                    for idx, path in enumerate(media_paths):
                        try:
                            # Attach caption only to the first photo
                            caption = master_message if idx == 0 else None
                            media_group.append(InputMediaPhoto(open(path, 'rb'), caption=caption, parse_mode="Markdown"))
                        except Exception as e:
                            logger.error(f"Error opening media path {path} for Yetal: {e}")
                            
                    if media_group:
                        msgs = await context.bot.send_media_group(chat_id=PRODUCT_CHANNEL_ID, media=media_group)
                        yetal_msg_id = msgs[0].message_id
                
                # Case 2: Single File ID
                elif product_data.get("photo_file_id"):
                    product_msg = await context.bot.send_photo(
                        chat_id=PRODUCT_CHANNEL_ID,  # This is Yetal_Search channel
                        photo=product_data["photo_file_id"],
                        caption=master_message,
                        parse_mode="Markdown",
                    )
                    yetal_msg_id = product_msg.message_id
                    
                # Case 3: Single Local File
                elif media_paths and len(media_paths) == 1:
                    product_msg = await context.bot.send_photo(
                        chat_id=PRODUCT_CHANNEL_ID,
                        photo=open(media_paths[0], 'rb'),
                        caption=master_message,
                        parse_mode="Markdown",
                    )
                    yetal_msg_id = product_msg.message_id
                    
                # Case 4: Text Only
                else:
                    product_msg = await context.bot.send_message(
                        chat_id=PRODUCT_CHANNEL_ID,
                        text=master_message,
                        parse_mode="Markdown",
                    )
                    yetal_msg_id = product_msg.message_id
                    
                logger.info(f"✅ Product posted to Yetal_Search channel: {PRODUCT_CHANNEL_ID}, Message ID: {yetal_msg_id}")
            except Exception as e:
                logger.error(f"❌ Failed to post to Yetal_Search channel {PRODUCT_CHANNEL_ID}: {e}")
                # Don't return failure here - continue

        return (
            True,
            target_channel_id,
            user_msg_id,  # User channel message ID
            master_message_id,  # Master data channel message ID
            yetal_msg_id,  # Yetal_Search channel message ID (this is what we want for product_ref)
            target_channel_username,  # Return target channel username
        )
    except Exception as e:
        logger.error(f"❌ Failed to post product: {e}")
        return False, None, None, None, None, None

# --- Channel Management ---

async def add_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Starts channel addition flow"""
    context.user_data["current_channel"] = {}
    await update.message.reply_text(
        "First make sure to add the bot as an administrator to the channel.\n"
        "Please send the channel username (e.g., @MyStore):"
    )
    return AWAITING_CHANNEL_USERNAME

async def handle_channel_username(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles channel username input"""
    username = update.message.text.strip().lower()
    if not username.startswith("@"):
        username = f"@{username}"

    try:
        chat = await context.bot.get_chat(username)
        context.user_data["current_channel"] = {
            "username": username,
            "channel_id": str(chat.id),
            "title": chat.title,
        }

        await update.message.reply_text(
            "📞 Please provide your business contact phone (e.g., +251912345678):"
        )
        return AWAITING_CHANNEL_CONTACT
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)}. Please try again:")
        return AWAITING_CHANNEL_USERNAME

async def handle_channel_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles channel contact input and asks for location."""
    phone = update.message.text.strip()
    if not validate_phone(phone):
        await update.message.reply_text("❌ Invalid phone format. Please try again:")
        return AWAITING_CHANNEL_CONTACT

    context.user_data["current_channel"]["contact"] = phone

    await update.message.reply_text(
        "Please type your exact business location (e.g., Dembel City Center 1st floor, Shop No. 124):"
    )
    return AWAITING_CHANNEL_LOCATION

async def _show_channel_confirmation(
    update: Update, context: ContextTypes.DEFAULT_TYPE, location_text: str
):
    """Helper function to show the final channel confirmation."""
    context.user_data["current_channel"]["location"] = location_text
    channel = context.user_data["current_channel"]

    keyboard = [
        [InlineKeyboardButton("✅ Confirm", callback_data="confirm_channel")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_channel")],
    ]

    await update.message.reply_text(
        f"Channel Setup:\n\n"
        f"Name: {channel['title']}\n"
        f"Username: {channel['username']}\n"
        f"Contact: {channel['contact']}\n"
        f"Location: {channel['location']}\n\n"
        "Confirm to save these settings:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return AWAITING_CHANNEL_CONFIRMATION

async def handle_channel_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles channel location from text input."""
    location = update.message.text.strip()
    return await _show_channel_confirmation(update, context, location)

async def confirm_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Confirms channel addition"""
    query = update.callback_query
    await query.answer()

    channel = context.user_data["current_channel"]

    # Save channel reference to master channel
    await _send_master_record(
        context.bot,
        "channel_ref",
        {
            "username": channel["username"],
            "contact": channel["contact"],
            "location": channel["location"],
        },
    )

    # Add to user's channels list
    context.user_data.setdefault("channels", []).append(channel)

    # Set the current_channel_data so the /addproduct command knows a channel is configured.
    context.user_data["current_channel_data"] = channel

    await query.edit_message_text(
        f"✅ Channel {channel['title']} setup complete!",
        reply_markup=None,  # Remove the inline keyboard
    )

    # Also remove the reply keyboard
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="You can now add products to this channel using /addproduct.",
        reply_markup=ReplyKeyboardRemove(),
    )

    # Insert channel data to MongoDB (only if not exists)
    if channels_collection is not None:
        try:
            # Check if channel already exists
            existing_channel = channels_collection.find_one({"username": channel["username"]})

            if existing_channel:
                logger.info(f"ℹ️ Channel {channel['username']} already exists in MongoDB, skipping insertion")
            else:
                # Insert new channel data
                channels_collection.insert_one({
                    "username": channel["username"],
                    "title": channel["title"]
                })
                logger.info(f"✅ Channel data inserted to MongoDB: {channel['username']} - {channel['title']}")
        except Exception as e:
            logger.error(f"❌ Failed to check/insert channel to MongoDB: {e}")

    # Note: Redis persistence handles data automatically, no manual sync needed

    return ConversationHandler.END

async def cancel_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancels channel addition"""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("❌ Channel setup cancelled.")
    return ConversationHandler.END

# --- Product Management ---

async def start_product_creation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Starts product creation flow"""
    if "current_channel_data" not in context.user_data:
        await update.message.reply_text(
            "❌ No channel configured. Please use /addchannel first."
        )
        return ConversationHandler.END

    context.user_data["current_product"] = {"user_id": update.effective_user.id}
    await update.message.reply_text("📷 Please send product image:")
    return AWAITING_IMAGE

async def handle_image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles product image"""
    try:
        photo = update.message.photo[-1]
        context.user_data["current_product"]["photo_file_id"] = photo.file_id
        await update.message.reply_text("📛 Enter product title:")
        return AWAITING_TITLE
    except Exception as e:
        logger.error(f"Image handling error: {e}")
        await update.message.reply_text("❌ Failed to process image. Please try again.")
        return ConversationHandler.END

async def handle_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles product title"""
    title = update.message.text.strip()
    if len(title) > MAX_TITLE_LENGTH:
        await update.message.reply_text(
            f"❌ Title too long (max {MAX_TITLE_LENGTH} chars). Please shorten it:"
        )
        return AWAITING_TITLE

    context.user_data["current_product"]["title"] = title
    await update.message.reply_text("📝 Enter product description:")
    return AWAITING_DESCRIPTION

async def handle_description(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles product description"""
    description = update.message.text.strip()
    if len(description) > MAX_DESCRIPTION_LENGTH:
        await update.message.reply_text(
            f"❌ Description too long (max {MAX_DESCRIPTION_LENGTH} chars). Please shorten it:"
        )
        return AWAITING_DESCRIPTION

    context.user_data["current_product"]["description"] = description
    await update.message.reply_text("💰 Enter product price (e.g., 49.99):")
    return AWAITING_PRICE

async def handle_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles product price"""
    try:
        price = float(update.message.text.strip())
        if price <= 0:
            raise ValueError("Price must be positive")

        context.user_data["current_product"]["price"] = price

        keyboard = [
            [InlineKeyboardButton("👁️ Show Price", callback_data="visible_1")],
            [InlineKeyboardButton("❌ Hide Price", callback_data="visible_0")],
        ]
        await update.message.reply_text(
            "Should the price be visible?", reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return AWAITING_PRICE_VISIBILITY
    except ValueError:
        await update.message.reply_text(
            "❌ Invalid price. Please enter a positive number:"
        )
        return AWAITING_PRICE

async def handle_price_visibility(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles price visibility selection"""
    query = update.callback_query
    await query.answer()

    visible = int(query.data.split("_")[1])
    context.user_data["current_product"]["price_visible"] = visible

    await query.edit_message_text(
        f"Price will be {'visible' if visible else 'hidden'}."
    )
    await context.bot.send_message(
        chat_id=update.effective_user.id, text="📦 Enter stock quantity or /skip:"
    )
    return AWAITING_STOCK

async def handle_stock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles stock quantity"""
    try:
        stock = int(update.message.text.strip())
        if stock < 0:
            await update.message.reply_text(
                "❌ Stock cannot be negative. Please try again:"
            )
            return AWAITING_STOCK

        await finish_product(update, context, stock)
        return ConversationHandler.END
    except ValueError:
        await update.message.reply_text(
            "❌ Invalid stock quantity. Please enter a number:"
        )
        return AWAITING_STOCK

async def skip_stock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Skips stock quantity"""
    await finish_product(update, context, -1)
    return ConversationHandler.END

# In the finish_product function, replace the UUID generation:
async def finish_product(update: Update, context: ContextTypes.DEFAULT_TYPE, stock: int):
    """Finalizes product creation and automatically saves to JSON"""
    product = context.user_data["current_product"]
    channel_data = context.user_data["current_channel_data"]
    product["stock"] = stock
    
    # Get the actual message ID from the posted message - FIXED: now unpacks 6 values
    success, channel_id, user_message_id, master_message_id, yetal_message_id, target_channel_username = (
        await post_product_to_channels(product, channel_data, context)
    )
    
    if success:
        # Use the Yetal_Search channel message ID as product reference (not user channel)
        if yetal_message_id:
            product["product_uuid"] = str(yetal_message_id)  # Use Yetal channel message ID
            product_ref = str(yetal_message_id)
            # Store both IDs for reference
            product["user_channel_message_id"] = user_message_id
            product["yetal_channel_message_id"] = yetal_message_id
        else:
            # Fallback to user channel message ID if Yetal message ID not available
            fallback_id = user_message_id if user_message_id else str(int(time.time() * 1000))
            product["product_uuid"] = str(fallback_id)
            product_ref = str(fallback_id)
            
        product["user_id"] = update.effective_user.id

        # --- AI ENHANCEMENT: Generate predicted_category and generated_description ---
        if HUGGINGFACE_API_TOKEN:
            try:
                # Combine title and description for classification
                input_text = f"{product.get('title', '')} {product.get('description', '')}".strip()
                
                # Define candidate categories
                candidate_labels = [
                    "Electronics",
                    "Clothing",
                    "Home & Kitchen", 
                    "Beauty & Personal Care",
                    "Sports & Outdoors",
                    "Books",
                    "Toys & Games",
                    "Automotive",
                    "Jewelry",
                    "Services",
                    "Other"
                ]
                
                # Classify product category using facebook/bart-large-mnli
                classification_payload = {
                    "inputs": input_text,
                    "parameters": {"candidate_labels": candidate_labels}
                }
                classification_result = call_huggingface_api("facebook/bart-large-mnli", classification_payload)
                if classification_result and isinstance(classification_result, dict):
                    product["predicted_category"] = classification_result.get("labels", ["Other"])[0]
                    logger.info(f"✅ Classified product '{product['title']}' as category: {product['predicted_category']}")
                else:
                    product["predicted_category"] = "Other"
                    logger.warning("⚠️ Failed to classify product category: API returned invalid response")
                
                # Generate detailed description using facebook/bart-large-cnn
                try:
                    summarization_payload = {
                        "inputs": product["description"],
                        "parameters": {"max_length": 150, "min_length": 30, "do_sample": False}
                    }
                    summarization_result = call_huggingface_api("facebook/bart-large-cnn", summarization_payload)
                    if summarization_result and isinstance(summarization_result, list) and len(summarization_result) > 0:
                        product["generated_description"] = summarization_result[0].get("summary_text", product["description"])
                        logger.info(f"✅ Generated detailed description for '{product['title']}'")
                    else:
                        product["generated_description"] = product["description"]
                        logger.warning("⚠️ Failed to generate detailed description: API returned invalid response")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to generate detailed description: {e}")
                    product["generated_description"] = product["description"]
            except Exception as e:
                logger.warning(f"⚠️ Failed to process product with Hugging Face APIs: {e}")
                product["predicted_category"] = "Other"
                product["generated_description"] = product["description"]
        else:
            logger.warning("⚠️ Hugging Face API token not set, skipping AI enhancements")
            product["predicted_category"] = "Other"
            product["generated_description"] = product["description"]

        # Store IDs for post links
        product["posted_message_id"] = user_message_id
        product["master_message_id"] = master_message_id
        product["target_channel_username"] = target_channel_username
        
        # Create proper post link using YETAL channel message ID
        if yetal_message_id:
            # Use Yetal_Search channel format for post links
            post_link = f"https://t.me/{YETAL_SEARCH_USERNAME}/{yetal_message_id}"
            logger.info(f"🔗 Post link created: {post_link} (Yetal message ID: {yetal_message_id})")
        else:
            # Fallback if Yetal message ID not available
            post_link = f"https://t.me/{YETAL_SEARCH_USERNAME}/{product_ref}"
            logger.warning(f"⚠️ Using fallback post link: {post_link}")

        # Store in recent products
        recent_product = {
            **product,
            "posted_channel_id": channel_id,
            "posted_message_id": user_message_id,
            "yetal_message_id": yetal_message_id,  # Store Yetal message ID separately
            "master_message_id": master_message_id,
            "product_channel_message_id": yetal_message_id,  # This is the Yetal channel message ID
            "channel_data": channel_data,
        }
        
        context.user_data.setdefault("recent_products", []).append(recent_product)
        
        # ✅ AUTOMATICALLY SAVE TO JSON
        logger.info("🔄 Starting automatic JSON save...")

        # Send verification check message
        verification_msg = await update.message.reply_text("🔍 Checking channel verification...")

        # Check channel verification from MongoDB
        channel_verified = False
        if channels_collection is not None:
            try:
                doc = channels_collection.find_one({"username": channel_data.get("username")})
                if doc and doc.get("isverified") == True:
                    channel_verified = True
                    logger.info(f"✅ Channel {channel_data.get('username')} is verified")
                else:
                    logger.info(f"⚠️ Channel {channel_data.get('username')} is not verified")
            except Exception as e:
                logger.error(f"❌ Error checking channel verification: {e}")

        # Prepare data for JSON storage with CORRECT FIELD NAMES AND FORMAT
        json_product_data = {
            "user_id": str(product["user_id"]),
            "title": product["title"],
            "description": product["description"],
            "price": str(product["price"]),
            "phone": channel_data.get("contact", "Not provided"),
            "images": [],
            "location": channel_data.get("location", ""),
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "channel": channel_data.get("username", "N/A"),
            "post_link": post_link,  # Use Yetal_Search channel format
            "product_ref": product_ref,  # Use Yetal channel message ID as reference
            "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "predicted_category": product["predicted_category"],
            "generated_description": product["generated_description"],
            "channel_verified": channel_verified
        }

        json_success = append_to_scraped_data(json_product_data)

        # Update verification message
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=verification_msg.message_id,
            text=f"✅ Channel verification complete. Verified: {channel_verified}"
        )
        
        # Append to JSON file in S3
        json_success = append_to_scraped_data(json_product_data)
        
        if json_success:
            logger.info("✅ Product successfully saved to JSON file!")
            await update.message.reply_text("✅ Product posted and saved successfully!")
        else:
            logger.warning("⚠️ Product posted but JSON save failed")
            await update.message.reply_text("✅ Product posted! (There was an issue saving to archive)")
    else:
        await update.message.reply_text("❌ Failed to post product. Please try again.")
    
    if "current_product" in context.user_data:
        del context.user_data["current_product"]
        
        # --- Product Editing ---

async def edit_product_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Starts the product editing flow"""
    try:
        recent = context.user_data.get("recent_products", [])
        if not recent:
            await update.message.reply_text(
                "No recent products found to edit. Add one using /addproduct first."
            )
            return ConversationHandler.END

        # Create keyboard with product titles
        keyboard = [
            [
                InlineKeyboardButton(
                    p.get("title", "Untitled Product")[:30],
                    callback_data=f"edit_{p['product_uuid']}",
                )
            ]
            for p in recent[-5:]  # Show only last 5 products to avoid button overflow
        ]

        # Add cancel button
        keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_edit")])

        await update.message.reply_text(
            "Which product do you want to edit?",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return EDIT_SELECT_PRODUCT
    except Exception as e:
        logger.error(f"Error in edit_product_command: {e}")
        await update.message.reply_text("❌ Error starting edit process. Please try again.")
        return ConversationHandler.END

async def edit_product_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the selection of a product to edit"""
    query = update.callback_query
    await query.answer()
    
    # Handle cancel case
    if query.data == "cancel_edit":
        await query.edit_message_text("❌ Edit cancelled.")
        return ConversationHandler.END
    
    try:
        product_uuid = query.data.split("edit_")[-1]
        
        # Find the selected product in recent_products
        product = next(
            (
                p
                for p in context.user_data.get("recent_products", [])
                if p.get("product_uuid") == product_uuid
            ),
            None,
        )

        if not product:
            await query.edit_message_text("❌ Product not found or may have been deleted.")
            return ConversationHandler.END

        # Store the product being edited
        context.user_data["editing_product"] = product
        context.user_data["editing_original"] = product.copy()

        await show_edit_options(query, context)
        return EDIT_FIELD_SELECT
    except Exception as e:
        logger.error(f"Error in edit_product_select: {e}")
        await query.edit_message_text("❌ Error selecting product. Please try again.")
        return ConversationHandler.END

async def show_edit_options(query, context: ContextTypes.DEFAULT_TYPE):
    """Shows the editing options with current values"""
    product = context.user_data["editing_product"]

    # Create the keyboard with current values
    keyboard = [
        [
            InlineKeyboardButton(
                f"✏️ Title: {product.get('title', '')[:20]}", callback_data="edit_title"
            )
        ],
        [
            InlineKeyboardButton(
                f"📝 Desc: {product.get('description', '')[:20]}...",
                callback_data="edit_description",
            )
        ],
        [
            InlineKeyboardButton(
                f"💰 Price: {product.get('price', 0)}", callback_data="edit_price"
            )
        ],
        [
            InlineKeyboardButton(
                f"👁️ Price Visible: {'Yes' if product.get('price_visible', 1) else 'No'}",
                callback_data="edit_visibility",
            )
        ],
        [
            InlineKeyboardButton(
                f"📦 Stock: {product.get('stock', 'N/A')}", callback_data="edit_stock"
            )
        ],
        [
            InlineKeyboardButton("✅ Confirm", callback_data="confirm_edit"),
            InlineKeyboardButton("❌ Cancel", callback_data="cancel_edit"),
        ],
    ]

    # Check if we have a query or update object
    if hasattr(query, "edit_message_text"):
        # It's a CallbackQuery
        await query.edit_message_text(
            "What do you want to edit? Current values shown:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    else:
        # It's an Update from a message handler
        await query.message.reply_text(
            "What do you want to edit? Current values shown:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

async def edit_field_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the selection of which field to edit"""
    query = update.callback_query
    await query.answer()

    action = query.data

    if action == "confirm_edit":
        return await confirm_edit(update, context)
    elif action == "cancel_edit":
        return await cancel_edit(update, context)

    field = action.split("_")[-1]
    product = context.user_data["editing_product"]

    if field == "visibility":
        # Toggle visibility immediately
        product["price_visible"] = 1 - product.get("price_visible", 1)
        await show_edit_options(query, context)
        return EDIT_FIELD_SELECT

    context.user_data["editing_field"] = field
    current_value = product.get(field, "")

    await query.edit_message_text(
        f"Current {field.replace('_', ' ')}: {current_value}\n"
        f"Enter new value for {field.replace('_', ' ')}:"
    )
    return EDIT_NEW_VALUE

async def apply_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Applies the edit and returns to edit options"""
    field = context.user_data.get("editing_field")
    new_value = update.message.text.strip()
    product = context.user_data["editing_product"]

    # Validate input
    if field == "title" and len(new_value) > MAX_TITLE_LENGTH:
        await update.message.reply_text(
            f"❌ Title too long (max {MAX_TITLE_LENGTH} chars). Try again:"
        )
        return EDIT_NEW_VALUE
    elif field == "description" and len(new_value) > MAX_DESCRIPTION_LENGTH:
        await update.message.reply_text(
            f"❌ Description too long (max {MAX_DESCRIPTION_LENGTH} chars). Try again:"
        )
        return EDIT_NEW_VALUE
    elif field == "price":
        try:
            new_value = float(new_value)
            if new_value <= 0:
                await update.message.reply_text("❌ Price must be positive. Try again:")
                return EDIT_NEW_VALUE
        except ValueError:
            await update.message.reply_text("❌ Invalid price. Enter a number:")
            return EDIT_NEW_VALUE
    elif field == "stock":
        try:
            new_value = int(new_value)
            if new_value < -1:
                await update.message.reply_text(
                    "❌ Stock must be -1 or greater. Try again:"
                )
                return EDIT_NEW_VALUE
        except ValueError:
            await update.message.reply_text("❌ Invalid stock. Enter a number:")
            return EDIT_NEW_VALUE

    # Update the product
    product[field] = new_value

    # Show edit options again - pass the update object
    await show_edit_options(update, context)
    return EDIT_FIELD_SELECT

async def confirm_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Finalizes product edits, updates channels, and updates JSON file"""
    query = update.callback_query
    await query.answer()
    edited_product = context.user_data["editing_product"]
    original_product = context.user_data["editing_original"]
    channel_data = edited_product.get("channel_data", {})
    user_id = update.effective_user.id
    # Ensure user_id is set
    edited_product["user_id"] = user_id

    # --- AI ENHANCEMENT: Generate predicted_category and generated_description ---
    if HUGGINGFACE_API_TOKEN:
        try:
            # Combine title and description for classification
            input_text = f"{edited_product.get('title', '')} {edited_product.get('description', '')}".strip()
            
            # Define candidate categories
            candidate_labels = [
                "Electronics",
                "Clothing",
                "Home & Kitchen", 
                "Beauty & Personal Care",
                "Sports & Outdoors",
                "Books",
                "Toys & Games",
                "Automotive",
                "Jewelry",
                "Services",
                "Other"
            ]
            
            # Classify product category using facebook/bart-large-mnli
            classification_payload = {
                "inputs": input_text,
                "parameters": {"candidate_labels": candidate_labels}
            }
            classification_result = call_huggingface_api("facebook/bart-large-mnli", classification_payload)
            if classification_result and isinstance(classification_result, dict):
                edited_product["predicted_category"] = classification_result.get("labels", ["Other"])[0]
                logger.info(f"✅ Classified edited product '{edited_product['title']}' as category: {edited_product['predicted_category']}")
            else:
                edited_product["predicted_category"] = "Other"
                logger.warning("⚠️ Failed to classify edited product category: API returned invalid response")
            
            # Generate detailed description using facebook/bart-large-cnn
            try:
                summarization_payload = {
                    "inputs": edited_product["description"],
                    "parameters": {"max_length": 150, "min_length": 30, "do_sample": False}
                }
                summarization_result = call_huggingface_api("facebook/bart-large-cnn", summarization_payload)
                if summarization_result and isinstance(summarization_result, list) and len(summarization_result) > 0:
                    edited_product["generated_description"] = summarization_result[0].get("summary_text", edited_product["description"])
                    logger.info(f"✅ Generated detailed description for edited product '{edited_product['title']}'")
                else:
                    edited_product["generated_description"] = edited_product["description"]
                    logger.warning("⚠️ Failed to generate detailed description: API returned invalid response")
            except Exception as e:
                logger.warning(f"⚠️ Failed to generate detailed description for edited product: {e}")
                edited_product["generated_description"] = edited_product["description"]
        except Exception as e:
            logger.warning(f"⚠️ Failed to process edited product with Hugging Face APIs: {e}")
            edited_product["predicted_category"] = "Other"
            edited_product["generated_description"] = edited_product["description"]
    else:
        logger.warning("⚠️ Hugging Face API token not set, skipping category and detailed description")
        edited_product["predicted_category"] = "Other"
        edited_product["generated_description"] = edited_product["description"]

    # Update recent_products list
    for p in context.user_data.get("recent_products", []):
        if p["product_uuid"] == edited_product["product_uuid"]:
            p.update(edited_product)
            updated_product = p
            break
    else:
        updated_product = edited_product
        
    try:
        # Build post link for the product channel message - USING TARGET CHANNEL FORMAT
        target_channel_username = edited_product.get("target_channel_username", channel_data.get("username", "").replace("@", ""))
        if target_channel_username and edited_product.get("posted_message_id"):
            user_channel_link = f"https://t.me/{target_channel_username}/{edited_product['posted_message_id']}"
        else:
            user_channel_link = f"https://t.me/Yetal_Search/{edited_product['product_uuid']}"
        
        # Prepare user caption (respects price visibility)
        user_caption = format_product_message(edited_product, channel_data)
        
        # Prepare master/product channel caption (always shows price) WITH POST LINK
        master_product = edited_product.copy()
        master_product["price_visible"] = 1
        master_caption = format_product_message(master_product, channel_data, user_channel_link)
        
        # === Post NEW message to PRODUCT channel (Yetal channel)
        try:
            # Post new message to Yetal channel with edited indicator at end
            success, _, _, _, yetal_msg_id, _ = await post_product_to_channels(
                edited_product, channel_data, context, is_edited=True
            )
            if success:
                logger.info("✅ New edited product posted to Yetal channel.")
                # Update the product data with new Yetal message ID
                edited_product["product_channel_message_id"] = yetal_msg_id
            else:
                logger.error("❌ Failed to post edited product to Yetal channel.")
        except Exception as e:
            logger.error(f"❌ Error posting to Yetal channel: {e}")

        # === Post NEW message to USER channel
        try:
            # Post new message to user channel
            user_message = format_product_message(edited_product, channel_data, is_edited=True)

            if edited_product.get("photo_file_id"):
                user_msg = await context.bot.send_photo(
                    chat_id=edited_product.get("posted_channel_id"),
                    photo=edited_product["photo_file_id"],
                    caption=user_message,
                    parse_mode="Markdown",
                )
            else:
                user_msg = await context.bot.send_message(
                    chat_id=edited_product.get("posted_channel_id"),
                    text=user_message,
                    parse_mode="Markdown"
                )

            # Update the message ID for future reference
            edited_product["posted_message_id"] = user_msg.message_id
            logger.info("✅ New edited product posted to user channel.")
        except Exception as e:
            logger.error(f"❌ Error posting to user channel: {e}")
                
        # === Log product edit as JSON to master data channel WITH AI FIELDS
        await _send_master_record(
            context.bot,
            "product_edit",
            {
                "title": edited_product.get("title"),
                "description": edited_product.get("description"),
                "price": edited_product.get("price"),
                "contact": channel_data.get("contact"),
                "location": channel_data.get("location"),
                "photo_file_id": edited_product.get("photo_file_id"),
                "username": channel_data.get("username"),
                # ✅ ADD AI FIELDS
                "predicted_category": edited_product.get("predicted_category"),
                "generated_description": edited_product.get("generated_description")
            },
            message_id=edited_product.get("product_uuid")  # Pass actual message ID
        )
        
        # ✅ UPDATE JSON FILE WITH EDITED PRODUCT
        logger.info("🔄 Updating JSON file with edited product...")

        # Check channel verification from MongoDB for edited product
        channel_verified = False
        if channels_collection is not None:
            try:
                doc = channels_collection.find_one({"username": channel_data.get("username")})
                if doc and doc.get("isverified") == True:
                    channel_verified = True
                    logger.info(f"✅ Channel {channel_data.get('username')} is verified (edit)")
                else:
                    logger.info(f"⚠️ Channel {channel_data.get('username')} is not verified (edit)")
            except Exception as e:
                logger.error(f"❌ Error checking channel verification for edit: {e}")

        # Prepare updated data for JSON with CORRECT FIELD NAMES AND FORMAT
        json_product_data = {
            "user_id": str(edited_product["user_id"]),
            "title": edited_product["title"],
            "description": edited_product["description"],
            "price": str(edited_product["price"]),
            "phone": channel_data.get("contact", "Not provided"),
            "images": [],
            "location": channel_data.get("location", ""),
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "channel": channel_data.get("username", "N/A"),
            "post_link": user_channel_link,  # Using the TARGET channel format
            "product_ref": edited_product["product_uuid"],  # TARGET channel message ID
            "last_edited": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "predicted_category": edited_product["predicted_category"],
            "generated_description": edited_product["generated_description"],
            "channel_verified": channel_verified
        }
        
        # ✅ UPDATE JSON FILE WITH EDITED PRODUCT
        logger.info("🔄 Updating JSON file with edited product...")

        # Check channel verification from MongoDB for edited product
        channel_verified = False
        if channels_collection is not None:
            try:
                doc = channels_collection.find_one({"username": channel_data.get("username")})
                if doc and doc.get("isverified") == True:
                    channel_verified = True
                    logger.info(f"✅ Channel {channel_data.get('username')} is verified (edit)")
                else:
                    logger.info(f"⚠️ Channel {channel_data.get('username')} is not verified (edit)")
            except Exception as e:
                logger.error(f"❌ Error checking channel verification for edit: {e}")

        # Prepare updated data for JSON with CORRECT FIELD NAMES AND FORMAT
        json_product_data = {
            "user_id": str(edited_product["user_id"]),
            "title": edited_product["title"],
            "description": edited_product["description"],
            "price": str(edited_product["price"]),
            "phone": channel_data.get("contact", "Not provided"),
            "images": [],
            "location": channel_data.get("location", ""),
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "channel": channel_data.get("username", "N/A"),
            "post_link": user_channel_link,  # Using the TARGET channel format
            "product_ref": edited_product["product_uuid"],  # TARGET channel message ID
            "last_edited": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "predicted_category": edited_product["predicted_category"],
            "generated_description": edited_product["generated_description"],
            "channel_verified": channel_verified
        }

        # Update existing record in JSON file instead of appending
        json_success = update_scraped_data(json_product_data)
        if json_success:
            logger.info("✅ Edited product updated in JSON file")
        else:
            logger.warning("⚠️ Product edited but JSON update failed")
            
        await query.edit_message_text("✅ Product updated successfully!")
        
    except Exception as e:
        logger.error(f"❌ Exception during confirm_edit: {e}")
        await query.edit_message_text("❌ Failed to update product.")
        
    # Cleanup
    context.user_data.pop("editing_product", None)
    context.user_data.pop("editing_original", None)
    context.user_data.pop("editing_field", None)
    return ConversationHandler.END

async def cancel_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancels the editing process"""
    query = update.callback_query
    await query.answer()

    # Clean up without saving changes
    if "editing_product" in context.user_data:
        del context.user_data["editing_product"]
    if "editing_original" in context.user_data:
        del context.user_data["editing_original"]
    if "editing_field" in context.user_data:
        del context.user_data["editing_field"]

    await query.edit_message_text("❌ Editing cancelled. No changes were made.")
    return ConversationHandler.END

# --- New S3 Status Commands ---

async def s3_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows S3 bucket connection status and file existence - FIXED for limited permissions"""
    # Check if user is admin
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ This command is only available for administrators.")
        return
    
    if not S3 or not AWS_BUCKET_NAME:
        await update.message.reply_text(
            "❌ S3 not configured properly\n\n"
            "🔧 *Configuration Status:*\n"
            f"AWS_BUCKET_NAME: `{AWS_BUCKET_NAME or 'NOT SET'}`\n"
            f"AWS_ACCESS_KEY_ID: `{'SET' if AWS_ACCESS_KEY_ID and len(AWS_ACCESS_KEY_ID) > 10 else 'INVALID/EMPTY'}`\n"
            f"AWS_SECRET_ACCESS_KEY: `{'SET' if AWS_SECRET_ACCESS_KEY and len(AWS_SECRET_ACCESS_KEY) > 10 else 'INVALID/EMPTY'}`\n"
            f"AWS_REGION: `{AWS_REGION or 'NOT SET'}`\n"
            f"S3 Client: `{'CREATED' if S3 else 'FAILED'}`\n\n"
            "💡 *Note:* Your IAM user doesn't have ListAllMyBuckets permission (this is normal for security)",
            parse_mode="Markdown"
        )
        return
    
    try:
        message = "🪣 *S3 Status Report*\n\n"
        
        # Test bucket connection with head_bucket
        try:
            S3.head_bucket(Bucket=AWS_BUCKET_NAME)
            message += "✅ *Bucket Connection:* Connected\n"
            message += f"📁 *Bucket Name:* `{AWS_BUCKET_NAME}`\n"
            message += f"🌍 *Region:* `{AWS_REGION}`\n\n"
        except Exception as e:
            error_code = e.response['Error']['Code'] if hasattr(e, 'response') else 'Unknown'
            message += f"⚠️ *Bucket Connection:* Limited - {error_code}\n"
            message += f"📁 *Bucket Name:* `{AWS_BUCKET_NAME}`\n"
            message += f"🌍 *Region:* `{AWS_REGION}`\n\n"
            message += "💡 *Note:* Cannot list all buckets (missing ListAllMyBuckets permission)\n\n"

        # Check session files - handle permission issues gracefully
        message += "📂 *Session Files:*\n"
        try:
            session_files = S3.list_objects_v2(Bucket=AWS_BUCKET_NAME, Prefix="sessions/", MaxKeys=10)
            session_items = session_files.get('Contents', [])
            
            user_session_exists = False
            
            for file in session_items:
                if file['Key'] == "sessions/user_session.pickle":
                    user_session_exists = True
                    message += f"  ✅ `user_session.pickle` - {file['Size']} bytes\n"
                else:
                    message += f"  📄 `{file['Key']}` - {file['Size']} bytes\n"
            
            if not user_session_exists:
                message += "  ❌ `user_session.pickle` - NOT FOUND\n"
                
            if not session_items:
                message += "  📭 No session files found\n"
                
        except Exception as e:
            message += f"  ⚠️ Cannot list session files: {str(e)[:100]}...\n"
        
        message += "\n"
        
        # Check data files
        message += "📊 *Data Files:*\n"
        try:
            # Check if JSON file exists
            try:
                json_info = S3.head_object(Bucket=AWS_BUCKET_NAME, Key="data/scraped_data.json")
                # Try to read and count records
                try:
                    data = load_json_from_s3("data/scraped_data.json")
                    message += f"  ✅ `scraped_data.json` - {json_info['ContentLength']} bytes, {len(data)} records\n"
                except:
                    message += f"  ✅ `scraped_data.json` - {json_info['ContentLength']} bytes (cannot read content)\n"
            except:
                message += "  ❌ `scraped_data.json` - NOT FOUND\n"
            
            # Check if forwarded messages JSON exists
            try:
                json_info = S3.head_object(Bucket=AWS_BUCKET_NAME, Key="data/forwarded_messages.json")
                message += f"  🔵 `forwarded_messages.json` - {json_info['ContentLength']} bytes\n"
            except:
                message += "  ℹ️ `forwarded_messages.json` - NOT FOUND\n"
                
        except Exception as e:
            message += f"  ⚠️ Cannot check data files: {str(e)[:100]}...\n"
        
        # Check Redis backup file
        message += "\n💾 *Redis Backup Files:*\n"
        try:
            backup_key = "redis_backups/user_data_backup.json"
            try:
                backup_info = S3.head_object(Bucket=AWS_BUCKET_NAME, Key=backup_key)
                backup_size = backup_info['ContentLength']
                last_modified = backup_info['LastModified']
                
                # Try to read and count user records in backup
                try:
                    response = S3.get_object(Bucket=AWS_BUCKET_NAME, Key=backup_key)
                    backup_json = response['Body'].read().decode('utf-8')
                    backup_data = json.loads(backup_json)
                    user_count = len(backup_data) if isinstance(backup_data, dict) else 0
                    message += f"  ✅ `{backup_key}`\n"
                    message += f"    📁 Size: {backup_size:,} bytes\n"
                    message += f"    👥 Users: {user_count} records\n"
                    message += f"    🕒 Last Modified: {last_modified.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
                except Exception as e:
                    message += f"  ✅ `{backup_key}`\n"
                    message += f"    📁 Size: {backup_size:,} bytes\n"
                    message += f"    🕒 Last Modified: {last_modified.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
                    message += f"    ⚠️ Cannot read backup content: {str(e)[:50]}...\n"
            except S3.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    message += f"  ❌ `{backup_key}` - NOT FOUND\n"
                    message += "    ℹ️ No backup created yet (will be created after first auto-backup)\n"
                else:
                    message += f"  ⚠️ `{backup_key}` - Error: {e.response['Error']['Code']}\n"
            except Exception as e:
                message += f"  ⚠️ Cannot check backup file: {str(e)[:100]}...\n"
        except Exception as e:
            message += f"  ⚠️ Error checking backup files: {str(e)[:100]}...\n"
        
        # Overall status
        message += f"\n📈 *Summary:* S3 client is running with limited permissions\n"
        message += f"💡 *IAM Policy Needed:* s3:GetObject, s3:PutObject, s3:DeleteObject, s3:ListBucket\n"
        
    except Exception as e:
        message = f"❌ *S3 Status Check Failed:* {str(e)[:200]}"
    
    await update.message.reply_text(message, parse_mode="Markdown")

async def json_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows detailed JSON file status and statistics"""
    # Check if user is admin
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ This command is only available for administrators.")
        return

    if not S3 or not AWS_BUCKET_NAME:
        await update.message.reply_text("❌ S3 not configured - missing client or bucket name")
        return

    try:
        message = "📊 *JSON File Status*\n\n"

        # Check if JSON file exists
        try:
            json_info = S3.head_object(Bucket=AWS_BUCKET_NAME, Key="data/scraped_data.json")
            json_size = json_info['ContentLength']
            last_modified = json_info['LastModified']

            message += "✅ *File Status:* EXISTS\n"
            message += f"📁 *File Size:* {json_size:,} bytes\n"
            message += f"🕒 *Last Modified:* {last_modified.strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"

            # Read JSON file for detailed stats
            data = load_json_from_s3("data/scraped_data.json")

            if data:
                message += "📈 *Data Statistics:*\n"
                message += f"  • Total Records: {len(data):,}\n"

                # Channel distribution
                channels = {}
                for item in data:
                    channel = item.get('channel', 'Unknown')
                    channels[channel] = channels.get(channel, 0) + 1

                message += "\n📺 *Channel Distribution:*\n"
                for channel, count in sorted(channels.items(), key=lambda x: x[1], reverse=True)[:5]:
                    percentage = (count / len(data)) * 100
                    # Escape any special characters in channel names
                    escaped_channel = channel.replace('*', '\\*').replace('_', '\\_').replace('`', '\\`')
                    message += f"  • {escaped_channel}: {count:,} ({percentage:.1f}%)\n"

                # Price statistics
                prices = [item.get('price', 0) for item in data if isinstance(item.get('price'), (int, float))]
                if prices:
                    message += "\n💰 *Price Statistics:*\n"
                    message += f"  • Min: ETB {min(prices):.2f}\n"
                    message += f"  • Max: ETB {max(prices):.2f}\n"
                    message += f"  • Avg: ETB {sum(prices)/len(prices):.2f}\n"

                # Date range
                dates = [item.get('date') for item in data if item.get('date')]
                if dates:
                    try:
                        date_objects = [datetime.strptime(date, '%Y-%m-%d %H:%M:%S') for date in dates if date]
                        if date_objects:
                            min_date = min(date_objects)
                            max_date = max(date_objects)
                            message += "\n📅 *Date Range:*\n"
                            message += f"  • From: {min_date.strftime('%Y-%m-%d')}\n"
                            message += f"  • To: {max_date.strftime('%Y-%m-%d')}\n"
                    except:
                        pass

                # Sample data - escape special characters in titles
                message += "\n👀 *Sample Data (first 3 products):*\n"
                for idx, item in enumerate(data[:3]):
                    title = item.get('title', 'N/A')[:30]
                    # Escape special characters in titles
                    escaped_title = title.replace('*', '\\*').replace('_', '\\_').replace('`', '\\`')
                    message += f"  {idx+1}. {escaped_title}... (ETB {item.get('price', 0):.2f})\n"

            else:
                message += "📭 *Data Status:* File is empty\n"

        except S3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                message += "❌ *File Status:* NOT FOUND\n"
                message += "The JSON file doesn't exist yet. It will be created when you add your first product.\n"
            else:
                message += f"❌ *File Status:* ERROR - {e.response['Error']['Code']}\n"
        except Exception as e:
            message += f"❌ *File Status:* ERROR - {str(e)}\n"

    except Exception as e:
        message = f"❌ *JSON Status Check Failed:* {str(e)}"

    # Send with parse_mode and handle potential errors
    try:
        await update.message.reply_text(message, parse_mode="Markdown")
    except BadRequest as e:
        # Fallback: send without Markdown formatting if there's still an issue
        logger.warning(f"Markdown parsing failed, sending as plain text: {e}")
        plain_message = message.replace('*', '').replace('_', '').replace('`', '')
        await update.message.reply_text(plain_message)

async def download_json(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Download the JSON data file from S3 (admin only)"""
    # Check if user is admin
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ This command is only available for administrators.")
        return

    if not S3 or not AWS_BUCKET_NAME:
        await update.message.reply_text("❌ S3 not configured - missing client or bucket name")
        return

    try:
        # Check if JSON file exists
        try:
            json_info = S3.head_object(Bucket=AWS_BUCKET_NAME, Key="data/scraped_data.json")
            json_size = json_info['ContentLength']

            # Check file size limit (Telegram has 50MB limit for documents)
            if json_size > 50 * 1024 * 1024:  # 50MB
                await update.message.reply_text(f"❌ File too large ({json_size:,} bytes). Maximum allowed is 50MB.")
                return

            # Download the file
            response = S3.get_object(Bucket=AWS_BUCKET_NAME, Key="data/scraped_data.json")
            file_content = response['Body'].read()

            # Create a temporary file
            import tempfile
            import os

            with tempfile.NamedTemporaryFile(mode='w+b', suffix='.json', delete=False) as temp_file:
                temp_file.write(file_content)
                temp_file_path = temp_file.name

            try:
                # Send the file as document
                with open(temp_file_path, 'rb') as file:
                    await update.message.reply_document(
                        document=file,
                        filename="scraped_data.json",
                        caption=f"📊 JSON Data Export\n📁 Size: {json_size:,} bytes\n🕒 Last Modified: {json_info['LastModified'].strftime('%Y-%m-%d %H:%M:%S UTC')}"
                    )
                await update.message.reply_text("✅ JSON data downloaded successfully!")
            finally:
                # Clean up temporary file
                os.unlink(temp_file_path)

        except S3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                await update.message.reply_text("❌ JSON file not found in S3. No data has been collected yet.")
            else:
                await update.message.reply_text(f"❌ Error accessing S3 file: {e.response['Error']['Code']}")
        except Exception as e:
            await update.message.reply_text(f"❌ Error downloading file: {str(e)}")

    except Exception as e:
        await update.message.reply_text(f"❌ Download failed: {str(e)}")

async def s3_storage_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows S3 storage usage and costs"""
    # Check if user is admin
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ This command is only available for administrators.")
        return
    
    if not S3 or not AWS_BUCKET_NAME:
        await update.message.reply_text("❌ S3 not configured - missing client or bucket name")
        return
    
    try:
        message = "💾 *S3 Storage Information*\n\n"
        
        # Get all objects in the bucket with pagination
        total_size = 0
        total_files = 0
        files_by_type = {}
        
        paginator = S3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=AWS_BUCKET_NAME):
            if 'Contents' in page:
                for obj in page['Contents']:
                    total_size += obj['Size']
                    total_files += 1
                    
                    # Categorize by file type
                    file_ext = obj['Key'].split('.')[-1].lower() if '.' in obj['Key'] else 'other'
                    files_by_type[file_ext] = files_by_type.get(file_ext, 0) + obj['Size']
        
        message += "📊 *Storage Summary:*\n"
        message += f"  • Total Files: {total_files:,}\n"
        message += f"  • Total Size: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)\n\n"
        
        message += "📁 *Storage by File Type:*\n"
        for file_type, size in sorted(files_by_type.items(), key=lambda x: x[1], reverse=True):
            percentage = (size / total_size) * 100 if total_size > 0 else 0
            message += f"  • {file_type.upper()}: {size:,} bytes ({percentage:.1f}%)\n"
        
        # Cost estimation (rough estimate)
        # S3 Standard Storage: ~$0.023 per GB per month
        monthly_cost = (total_size / 1024 / 1024 / 1024) * 0.023
        message += f"\n💰 *Cost Estimation:*\n"
        message += f"  • Monthly Storage: ${monthly_cost:.4f}\n"
        message += f"  • Based on S3 Standard pricing ($0.023/GB-month)\n"
        
        message += f"\n💡 *Tips:*\n"
        message += f"  • Consider lifecycle rules for old data\n"
        message += f"  • Monitor growth patterns\n"
        message += f"  • Use S3 Intelligent-Tiering for cost optimization\n"
        
    except Exception as e:
        message = f"❌ *Storage Info Failed:* {str(e)}"
    
    await update.message.reply_text(message, parse_mode="Markdown")

# --- Keep Alive Functions ---

def start_keep_alive():
    """Start background thread to keep the bot alive by pinging health endpoint"""
    def keep_alive_worker():
        while True:
            try:
                # Ping the health endpoint to keep the service awake
                response = requests.get(f"{HEALTH_CHECK_URL}/health", timeout=10)
                if response.status_code == 200:
                    logger.info(f"✅ Keep-alive ping successful: {response.status_code}")
                else:
                    logger.warning(f"⚠️ Keep-alive ping returned: {response.status_code}")
            except Exception as e:
                logger.warning(f"⚠️ Keep-alive ping failed: {e}")
            
            # Wait for the specified interval
            time.sleep(KEEP_ALIVE_INTERVAL)
    
    # Start the keep-alive thread
    keep_alive_thread = threading.Thread(target=keep_alive_worker, daemon=True)
    keep_alive_thread.start()
    logger.info(f"🔄 Keep-alive service started (pinging every {KEEP_ALIVE_INTERVAL} seconds)")

# --- Command Handlers ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Welcome message"""
    user_id = update.effective_user.id
    is_user_admin = is_admin(user_id)
    
    message = (
        "🛍️ Welcome to the Product Bot!\n\n"
        "Use /addchannel to set up your sales channel first\n"
        "Then use /addproduct to create listings\n"
        "/editproduct to modify existing products\n\n"
    )
    
    # Only show admin commands to admins
    if is_user_admin:
        message += (
            "*Admin Commands:*\n"
            "/s3status - S3 bucket and file status\n" 
            "/jsonstatus - Detailed JSON file info\n"
            "/s3storage - Storage usage and costs\n\n"
        )
    
    message += "/help for all commands"
    
    await update.message.reply_text(message, parse_mode="Markdown")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Help message"""
    user_id = update.effective_user.id
    is_user_admin = is_admin(user_id)
    
    help_text = (
        "📚 *Available Commands:*\n\n"
        "v1.0\n"
        "*Basic Commands:*\n"
        "/start - Welcome message\n"
        "/help - This message\n"
        "/addchannel - Set up your sales channel (required first)\n"
        "/addproduct - Create a new product listing\n"
        "/editproduct - Modify an existing product\n"
        "/myproducts - View your recent products\n\n"
    )
    
    # Only show admin commands to admins
    if is_user_admin:
        help_text += (
            "*Admin Commands:*\n"
            "/s3status - S3 bucket connection and file status\n"
            "/jsonstatus - Detailed JSON file statistics\n" 
            "/s3storage - Storage usage and cost information\n"
            "/repost - Repost content from another channel\n\n"
        )
    
    help_text += "Channel contact and location will be used for all products."

    # Only show admin commands to admins
    if is_user_admin:
        help_text += (
            "*Data Management Commands:*\n"
            "/downloadjson - Download the complete JSON data file from S3\n\n"
        )

    await update.message.reply_text(help_text, parse_mode="Markdown")

async def my_products(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Lists user's recent products"""
    recent = context.user_data.get("recent_products", [])
    if not recent:
        await update.message.reply_text("You haven't posted any products yet.")
        return

    response = "🛍️ Your Recent Products:\n\n"
    for idx, product in enumerate(recent, 1):
        response += f"{idx}. {product.get('title', 'Untitled')}\n"

    await update.message.reply_text(response)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancels any ongoing operation"""
    # Clean up any temporary channel data
    if "current_channel" in context.user_data:
        del context.user_data["current_channel"]

    await update.message.reply_text("❌ Operation cancelled.")
    return ConversationHandler.END

# --- Repost Command Handlers ---

async def start_repost(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Starts the repost command flow"""
    user_id = update.effective_user.id
    logger.info(f"🔄 User {user_id} requested /repost")
    
    if not is_admin(user_id):
        logger.warning(f"⚠️ User {user_id} denied access to /repost")
        await update.message.reply_text("❌ This command is only available for administrators.")
        return

    # Check for arguments: /repost @channel
    if context.args:
        username = context.args[0]
        if not username.startswith("@"):
            username = f"@{username}"
        context.user_data["repost_channel"] = username
        
        # Skip to days selection
        keyboard = [
            [InlineKeyboardButton("7 Days", callback_data="days_7")],
            [InlineKeyboardButton("10 Days", callback_data="days_10")],
            [InlineKeyboardButton("20 Days", callback_data="days_20")]
        ]
        await update.message.reply_text(
            f"📅 Channel set to {username}. How many days of content do you want to repost?",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return REPOST_SELECT_DAYS

    # No argument, check for persistent channel
    stored_channel = context.user_data.get("repost_channel")
    if stored_channel:
        keyboard = [
            [InlineKeyboardButton(f"Yes, use {stored_channel}", callback_data="use_stored_channel")],
            [InlineKeyboardButton("No, change channel", callback_data="change_channel")]
        ]
        await update.message.reply_text(
            f"🔄 Last used channel: {stored_channel}. Use this one?",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return REPOST_CONFIRM

    await update.message.reply_text(
        "📡 Please enter the source channel username (e.g., @OriginalChannel):"
    )
    return REPOST_GET_CHANNEL

async def handle_repost_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles confirmation of stored channel"""
    query = update.callback_query
    await query.answer()

    if query.data == "use_stored_channel":
        username = context.user_data.get("repost_channel")
        keyboard = [
            [InlineKeyboardButton("7 Days", callback_data="days_7")],
            [InlineKeyboardButton("10 Days", callback_data="days_10")],
            [InlineKeyboardButton("20 Days", callback_data="days_20")]
        ]
        await query.edit_message_text(
            f"📅 Channel set to {username}. How many days of content do you want to repost?",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return REPOST_SELECT_DAYS
    
    elif query.data == "change_channel":
        await query.edit_message_text(
            "📡 Please enter the new source channel username (e.g., @OriginalChannel):"
        )
        return REPOST_GET_CHANNEL

async def handle_repost_get_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles channel input"""
    username = update.message.text.strip()
    if not username.startswith("@"):
        username = f"@{username}"
    
    context.user_data["repost_channel"] = username
    
    keyboard = [
        [InlineKeyboardButton("7 Days", callback_data="days_7")],
        [InlineKeyboardButton("10 Days", callback_data="days_10")],
        [InlineKeyboardButton("20 Days", callback_data="days_20")]
    ]
    await update.message.reply_text(
        f"📅 Channel set to {username}. How many days of content do you want to repost?",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return REPOST_SELECT_DAYS

async def handle_repost_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles day selection and triggers scraping"""
    query = update.callback_query
    await query.answer()
    
    days = int(query.data.split("_")[1])
    username = context.user_data.get("repost_channel")
    
    await query.edit_message_text(
        f"⏳ Starting scraping from {username} for the last {days} days...\n"
        "This may take a while depending on the number of messages."
    )
    
    # Run scraping
    try:
        success, result = await scraper.scrape_channel_async(username, days)
        
        if not success:
            await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Scraping failed: {result}")
            return ConversationHandler.END
            
        items = result
        if not items:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="📭 No items found in the specified period.")
            return ConversationHandler.END
            
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"✅ Found {len(items)} items. Starting upload and repost process...")
        
        # Process items
        processed_count = 0
        
        # We define a dummy channel data for the source for display purposes
        channel_data = {
            "username": username,
            "title": f"Scraped from {username}",
            "contact": "See original",
            "location": "See original",
            "channel_id": None # No posting to user channel, only to Product Channel
        }
        
        for item in items:
            # Basic product structure
            product_data = {
                "title": item["title"],
                "description": item["description"],
                "price": item["price"] if item["price"] else 0,
                "price_visible": 1,
                "stock": 1,
                "predicted_category": "Other", 
                "generated_description": item["description"],
                "media_paths": item.get("media_paths", ([item["media_path"]] if item.get("media_path") else []))
            }
            
            try:
                # Use common posting function (it will handle proper JSON saving and channel posting)
                # Note: channel_id is None, so it won't post to a user channel, but WILL post to PRODUCT_CHANNEL_ID
                success, _, _, _, yetal_msg_id, _ = await post_product_to_channels(product_data, channel_data, context, is_edited=False)
                
                if success:
                    # Save to JSON manually since we're in a loop content
                    # But wait, post_product_to_channels sends to Master Channel, but does NOT save to scraped_data.json
                    # finish_product does the saving. So we need to save here.
                    
                    json_product_data = {
                        "user_id": str(update.effective_user.id),
                        "title": product_data["title"],
                        "description": product_data["description"],
                        "price": str(product_data["price"]),
                        "phone": item["phone"],
                        "location": item["location"],
                        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "channel": username,
                        "post_link": f"https://t.me/{YETAL_SEARCH_USERNAME}/{yetal_msg_id}" if yetal_msg_id else f"https://t.me/{username.replace('@','')}/{item['product_ref']}",
                        "product_ref": str(yetal_msg_id) if yetal_msg_id else item["product_ref"],
                        "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "predicted_category": "Other",
                        "generated_description": product_data["description"],
                        "channel_verified": False,
                         "has_media_group": len(product_data["media_paths"]) > 1
                    }
                    append_to_scraped_data(json_product_data)
                    processed_count += 1
                    
            except Exception as e:
                logger.error(f"Failed to process item {item['title']}: {e}")
            
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"✅ Reposting complete! Processed {processed_count} items.")
        
        # Cleanup
        try:
            shutil.rmtree("telegram_cache")
        except:
            pass
            
    except Exception as e:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Error: {e}")
        
    return ConversationHandler.END

async def cancel_repost(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancels repost"""
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text("❌ Repost cancelled.")
    else:
        await update.message.reply_text("❌ Repost cancelled.")
    return ConversationHandler.END

# --- Application Setup ---

def main():
    """Configures and runs the bot"""
    try:
        # Fix HTTPXRequest with required parameters
        request = HTTPXRequest(
            connection_pool_size=10,
            read_timeout=30.0,
        )
        
        # Set up Redis persistence for session data
        try:
            if REDIS_URL:
                persistence = RedisPersistence(
                    url=REDIS_URL,
                    prefix="telegram_bot"  # Optional prefix for keys
                )
                logger.info(f"✅ Redis persistence initialized with URL")
            else:
                logger.warning("⚠️ REDIS_URL not set, falling back to no persistence")
                persistence = None
        except Exception as e:
            logger.error(f"❌ Failed to initialize Redis persistence: {e}")
            # Fallback to no persistence
            persistence = None
        
    except Exception as e:
        logger.error(f"Failed to initialize persistence: {e}")
        # Fallback to no persistence
        persistence = None
        request = HTTPXRequest()  # Default request
    
    application = Application.builder() \
        .token(BOT_TOKEN) \
        .request(request) \
        .persistence(persistence) \
        .build()

    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("myproducts", my_products))
    application.add_handler(CommandHandler("cancel", cancel))
    
    # New S3 status commands
    application.add_handler(CommandHandler("s3status", s3_status))
    application.add_handler(CommandHandler("jsonstatus", json_status))
    application.add_handler(CommandHandler("s3storage", s3_storage_info))
    application.add_handler(CommandHandler("downloadjson", download_json))

    # Channel addition conversation
    channel_conv = ConversationHandler(
        entry_points=[CommandHandler("addchannel", add_channel)],
        states={
            AWAITING_CHANNEL_USERNAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_channel_username)
            ],
            AWAITING_CHANNEL_CONTACT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_channel_contact)
            ],
            AWAITING_CHANNEL_LOCATION: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND, handle_channel_location
                ),
            ],
            AWAITING_CHANNEL_CONFIRMATION: [
                CallbackQueryHandler(confirm_channel, pattern="^confirm_channel$"),
                CallbackQueryHandler(cancel_channel, pattern="^cancel_channel$"),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="channel_conversation",
        persistent=True,
    )

    # Product creation conversation
    product_conv = ConversationHandler(
        entry_points=[CommandHandler("addproduct", start_product_creation)],
        states={
            AWAITING_IMAGE: [
                MessageHandler(filters.PHOTO, handle_image),
                MessageHandler(filters.ALL, cancel),
            ],
            AWAITING_TITLE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_title)
            ],
            AWAITING_DESCRIPTION: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_description)
            ],
            AWAITING_PRICE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_price)
            ],
            AWAITING_PRICE_VISIBILITY: [
                CallbackQueryHandler(handle_price_visibility)
            ],
            AWAITING_STOCK: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_stock),
                CommandHandler("skip", skip_stock),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="product_conversation",
        persistent=True,
    )

    # Add this to your main application setup
    edit_conv = ConversationHandler(
        entry_points=[CommandHandler("editproduct", edit_product_command)],
        states={
            EDIT_SELECT_PRODUCT: [
                CallbackQueryHandler(edit_product_select, pattern="^edit_"),
                CallbackQueryHandler(cancel_edit, pattern="^cancel_edit$"),
            ],
            EDIT_FIELD_SELECT: [
                CallbackQueryHandler(edit_field_choice),
            ],
            EDIT_NEW_VALUE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, apply_edit),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CommandHandler("help", help_command),
            MessageHandler(filters.ALL, cancel)  # Catch-all fallback
        ],
        name="edit_conversation",
        persistent=True,
        allow_reentry=True
    )

# Don't forget to add this to your application:
# application.add_handler(edit_conv)

    # Repost conversation
    repost_conv = ConversationHandler(
        entry_points=[CommandHandler("repost", start_repost)],
        states={
            REPOST_GET_CHANNEL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_repost_get_channel)
            ],
            REPOST_CONFIRM: [
                 CallbackQueryHandler(handle_repost_confirm, pattern="^use_stored_channel$|^change_channel$")
            ],
            REPOST_SELECT_DAYS: [
                CallbackQueryHandler(handle_repost_days, pattern="^days_")
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel_repost)],
    )

    application.add_handler(channel_conv)
    application.add_handler(product_conv)
    application.add_handler(edit_conv)
    application.add_handler(repost_conv)
    
    # Start independent keep-alive thread
    if HEALTH_CHECK_URL:
        start_keep_alive()

    # Run the bot
    if not BOT_TOKEN:
        logger.error("❌ Bot token not found! Set TELEGRAM_BOT_TOKEN environment variable.")
        return

    logger.info("🤖 Bot is starting...")
    
    # Start auto-backup task after application is initialized
    async def start_auto_backup_task(app: Application):
        """Start the auto-backup task when application initializes (post_init callback)"""
        if persistence and hasattr(persistence, 'auto_backup'):
            try:
                # Create task for auto-backup in the running event loop
                asyncio.create_task(persistence.auto_backup())
                logger.info("✅ Auto-backup task started (runs every 20 minutes)")
            except Exception as e:
                logger.error(f"❌ Failed to start auto-backup: {e}")
    
    # Set post_init callback to start auto-backup (works without JobQueue)
    if persistence and hasattr(persistence, 'auto_backup'):
        application.post_init = start_auto_backup_task
    
    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

# === Flask helper integration (Render keep-alive) ===
def create_flask_app():
    """Create and configure Flask app for Render"""
    app = Flask(__name__)
    
    @app.route('/')
    def home():
        return "🛍️ Telegram Product Bot is running!"
    
    @app.route('/health')
    def health():
        return "OK", 200
    
    @app.route('/ping')
    def ping():
        return "pong", 200
    
    @app.route('/keepalive')
    def keepalive():
        return "🔄 Bot is alive and refreshing every 5 minutes", 200
    
    return app

def run_flask_app():
    """Run Flask app in a separate thread"""
    app = create_flask_app()
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

def run_bot_with_flask():
    """Run both bot and Flask app"""
    # Start Flask in a separate thread
    flask_thread = threading.Thread(target=run_flask_app, daemon=True)
    flask_thread.start()
    
    # Run the bot in main thread
    main()

if __name__ == "__main__":
    run_bot_with_flask()