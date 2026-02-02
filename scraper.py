
import os
import asyncio
import re
from datetime import datetime, timedelta, timezone
from telethon import TelegramClient
from telethon.sessions import SQLiteSession
from telethon.errors import ChannelInvalidError, UsernameInvalidError, UsernameNotOccupiedError
import boto3
import logging
import platform

# Configure logging
logger = logging.getLogger(__name__)

# Load environment variables
API_ID = 24916488
API_HASH = "3b7788498c56da1a02e904ff8e92d494"
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

# Initialize S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def get_session_filename():
    """Get unique session filename for each environment"""
    if 'RENDER' in os.environ:
        return "session_render.session"
    
    computer_name = platform.node().lower()
    username = os.getenv('USER', '').lower()
    
    local_indicators = ['desktop', 'laptop', 'pc', 'home', 'workstation', 'macbook']
    
    if any(indicator in computer_name for indicator in local_indicators):
        return "session_local.session"
    elif username and username not in ['render', 'root', 'admin']:
        return "session_local.session"
    elif os.name == 'nt':
        return "session_local.session"
    else:
        return "session_main.session"

USER_SESSION_FILE = get_session_filename()

async def get_telethon_client():
    """Get the main Telethon client with READ-ONLY session handling"""
    client = None
    max_retries = 3
    retry_delay = 2
    
    # Download session file from S3 to memory
    session_data = None
    try:
        response = s3.get_object(Bucket=AWS_BUCKET_NAME, Key=f"sessions/{USER_SESSION_FILE}")
        session_data = response['Body'].read()
        logger.info(f"✅ Downloaded session file from S3: {USER_SESSION_FILE}")
    except s3.exceptions.NoSuchKey:
        logger.error(f"❌ Session file not found in S3: {USER_SESSION_FILE}")
        return None
    except Exception as e:
        logger.error(f"❌ Error downloading session from S3: {e}")
        return None
    
    for attempt in range(max_retries):
        try:
            logger.info(f"🔧 Attempt {attempt + 1}/{max_retries} to connect Telethon client...")
            
            # Create a temporary writable session file
            temp_session_file = f"temp_{USER_SESSION_FILE}"
            with open(temp_session_file, 'wb') as f:
                f.write(session_data)
            
            # Use the temporary session file
            session = SQLiteSession(temp_session_file)
            client = TelegramClient(session, API_ID, API_HASH)
            
            await asyncio.wait_for(client.connect(), timeout=15)
            
            if not await client.is_user_authorized():
                error_msg = "Session not authorized"
                logger.error(f"❌ {error_msg}")
                await client.disconnect()
                if os.path.exists(temp_session_file):
                    os.remove(temp_session_file)
                return None
            
            me = await asyncio.wait_for(client.get_me(), timeout=10)
            logger.info(f"✅ Telethon connected successfully as: {me.first_name} (@{me.username})")
            
            client.temp_session_file = temp_session_file
            return client
            
        except Exception as e:
            error_msg = f"Connection error (attempt {attempt + 1}): {str(e)}"
            logger.error(f"❌ {error_msg}")
            
            if client:
                try:
                    await client.disconnect()
                except:
                    pass
            
            if 'temp_session_file' in locals() and os.path.exists(temp_session_file):
                try:
                    # Check if file is locked before removing
                    os.remove(temp_session_file)
                except:
                    pass
            
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                return None
    
    return None

def clean_text(text):
    if not text:
        return ""
    # Only normalize non-breaking spaces, preserve newlines
    return text.replace('\xa0', ' ')

def extract_info(text, message_id):
    if not text:
        return {
            "title": "No Title",
            "description": "",
            "price": "",
            "phone": "",
            "location": "",
            "channel_mention": "",
            "product_ref": str(message_id)
        }

    # Text is already reasonably clean, just strip edges
    text = clean_text(text).strip()
    
    # Extract Title (first line or until price/special chars)
    # matching until newline OR price markers
    title_match = re.split(r'\n|💸|☘️☘️PRICE|Price\s*:|💵', text)[0].strip()
    title = title_match[:100] if title_match else "No Title"
    
    # Match multiple phone number formats: +251..., 251..., 09..., +2519...
    phone_matches = re.findall(r'(\+?251\d{9}|\+251\d{8}|09\d{8})', text)
    phone = phone_matches[0] if phone_matches else ""
    
    # Match multiple price formats: Price: 100, 💰 Price: 100, ETB 100, 100 ETB, etc.
    price_match = re.search(
        r'(Price|💸|💰|☘️☘️PRICE)[:\s]*([\d,]+)|([\d,]+)\s*(ETB|Birr|birr|💵)', 
        text, 
        re.IGNORECASE
    )
    price = ""
    if price_match:
        price = price_match.group(2) or price_match.group(3) or ""
        price = price.replace(',', '').strip()
    
    location_match = re.search(
        r'(📍|Address|Location|🌺🌺)[:\s]*(.+?)(?=\n|☘️|📞|@|$)', 
        text, 
        re.IGNORECASE
    )
    if location_match:
        location = location_match.group(2).strip()
        # Remove any duplicate "Location:" prefix that might be in the extracted text
        location = re.sub(r'^(Location|Address|📍|🌺🌺)\s*:\s*', '', location, flags=re.IGNORECASE).strip()
    else:
        location = ""
    
    channel_mention = re.search(r'(@\w+)', text)
    channel_mention = channel_mention.group(1) if channel_mention else ""

    # Clean description by removing extracted metadata lines
    clean_description = text
    
    # Remove title if it's at the start
    if title_match and clean_description.startswith(title_match):
        clean_description = clean_description[len(title_match):].strip()
    elif title_match:
         clean_description = clean_description.replace(title_match, "", 1)
    
    # Remove metadata patterns from description to avoid duplication
    metadata_patterns = [
        # Match lines starting with price markers, regardless of content
        r'^(Price|💸|💰|☘️☘️PRICE).*$',
        # Match lines starting with location markers
        r'^(📍|Address|Location|🌺🌺).*$',
        # Match lines starting with contact markers
        r'^(📞|Contact|Phone).*$',
        # Match lines starting with stock markers
        r'^(📦|Stock).*$',
        # Specific extracted values (in case they are embedded in text)
        r'(\+?251\d{9}|\+251\d{8}|09\d{8})',
        r'(@\w+)'
    ]
    
    for pattern in metadata_patterns:
        # Use multiline flag to match ^ to start of lines
        clean_description = re.sub(pattern, "", clean_description, flags=re.IGNORECASE | re.MULTILINE)
        
    # Extra cleanup for empty lines left behind
    lines = [line.strip() for line in clean_description.split('\n')]
    clean_description = "\n".join([line for line in lines if line])
    
    # Clean up extra whitespace and newlines (preserve paragraph structure)
    # Remove single empty lines but keep double newlines (paragraphs)? 
    # Or just strip leading/trailing from lines and remove purely empty lines.
    lines = [line.strip() for line in clean_description.split('\n')]
    clean_description = "\n".join([line for line in lines if line])
    
    return {
        "title": title,
        "description": clean_description,
        "price": price,
        "phone": phone,
        "location": location,
        "channel_mention": channel_mention,
        "product_ref": str(message_id) 
    }

async def scrape_channel_async(channel_username: str, days: int):
    """
    Scrape a channel for the last N days.
    Returns a list of dictionaries containing message data and local file paths.
    """
    telethon_client = None
    scraped_items = []
    
    try:
        telethon_client = await get_telethon_client()
        if not telethon_client:
            return False, "❌ Could not establish Telethon connection."
        
        try:
            entity = await telethon_client.get_entity(channel_username)
            logger.info(f"✅ Found channel: {entity.title}")
        except Exception as e:
            return False, f"❌ Check channel username: {e}"

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=days)
        logger.info(f"⏰ Scraping messages since: {cutoff}")

        temp_dir = "telegram_cache"
        os.makedirs(temp_dir, exist_ok=True)

        # Buffer for grouped messages (albums)
        grouped_messages = {}
        
        # We need to iterate in reverse order (oldest to newest) to properly reconstruct albums if we were streaming,
        # but iter_messages goes new->old. We'll collect all relevant messages first.
        messages_to_process = []
        async for message in telethon_client.iter_messages(entity, limit=None):
            if message.date < cutoff:
                break
            # Skip service messages
            if message.action:
                continue
            messages_to_process.append(message)
            
        # Process messages from newest to oldest (default order from iter_messages)
        # But we need to group them.
        
        processed_group_ids = set()

        for message in messages_to_process:
            if not message.text and not message.media:
                continue

            # Handle grouped messages (albums)
            if message.grouped_id:
                if message.grouped_id in processed_group_ids:
                    continue
                
                # Find all messages in this group from our fetched list
                group_msgs = [m for m in messages_to_process if m.grouped_id == message.grouped_id]
                processed_group_ids.add(message.grouped_id)
                
                # Sort by id (usually implies order)
                group_msgs.sort(key=lambda x: x.id)
                
                # Use the first message (or the one with text) as the main one
                main_msg = next((m for m in group_msgs if m.text), group_msgs[0])
                info = extract_info(main_msg.text, main_msg.id)
                
                media_paths = []
                for g_msg in group_msgs:
                    if g_msg.media:
                        try:
                            filename = f"{channel_username}_{g_msg.id}"
                            path = await g_msg.download_media(file=os.path.join(temp_dir, filename))
                            if path:
                                media_paths.append(path)
                        except Exception as e:
                            logger.error(f"Failed to download media for {g_msg.id}: {e}")
                
                item = {
                    "title": info["title"],
                    "description": info["description"],
                    "price": info["price"],
                    "phone": info["phone"],
                    "location": info["location"],
                    "original_date": main_msg.date.isoformat(),
                    "original_link": f"https://t.me/{channel_username.replace('@', '')}/{main_msg.id}",
                    "media_path": media_paths[0] if media_paths else None,
                    "media_paths": media_paths, # New field for list of paths
                    "product_ref": str(main_msg.id),
                    "source_channel": channel_username
                }
                scraped_items.append(item)
                
            else:
                # Single message
                info = extract_info(message.text, message.id)
                
                # Download media if exists
                media_path = None
                media_paths = []
                if message.media:
                    try:
                        filename = f"{channel_username}_{message.id}"
                        path = await message.download_media(file=os.path.join(temp_dir, filename))
                        media_path = path
                        if path:
                            media_paths.append(path)
                    except Exception as e:
                        logger.error(f"Failed to download media for {message.id}: {e}")

                item = {
                    "title": info["title"],
                    "description": info["description"],
                    "price": info["price"],
                    "phone": info["phone"],
                    "location": info["location"],
                    "original_date": message.date.isoformat(),
                    "original_link": f"https://t.me/{channel_username.replace('@', '')}/{message.id}",
                    "media_path": media_path,
                    "media_paths": media_paths,
                    "product_ref": str(message.id),
                    "source_channel": channel_username
                }
                scraped_items.append(item)
            
        logger.info(f"✅ Scraped {len(scraped_items)} items")
        return True, scraped_items

    except Exception as e:
        logger.error(f"❌ Critical scraping error: {e}")
        return False, f"Error: {str(e)}"
    finally:
        if telethon_client:
            await telethon_client.disconnect()
            if hasattr(telethon_client, 'temp_session_file') and os.path.exists(telethon_client.temp_session_file):
                try:
                    os.remove(telethon_client.temp_session_file)
                except:
                    pass
