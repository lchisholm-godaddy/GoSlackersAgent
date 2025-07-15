import logging
import os
import json
import time
import re
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set, Any
from dataclasses import dataclass, asdict, field
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from fuzzywuzzy import fuzz, process
from cache_manager import SlackCacheManager

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Message:
    """Data class for storing message information"""
    id: str
    channel_id: str
    channel_name: str
    user_id: str
    username: str
    timestamp: str
    text: str
    message_type: str
    subtype: Optional[str] = None
    thread_ts: Optional[str] = None
    is_thread_parent: bool = False
    reply_count: int = 0
    reactions: List[Dict[str, Any]] = field(default_factory=list)
    attachments: List[Dict[str, Any]] = field(default_factory=list)

@dataclass
class Channel:
    """Data class for storing channel information"""
    id: str
    name: str
    purpose: str
    topic: str
    member_count: int
    is_private: bool
    is_archived: bool
    is_member: bool
    created: str
    creator: str

class SlackDataExtractor:
    """Main class for extracting and processing Slack data"""
    
    def __init__(self, bot_token: str, load_users: bool = False, use_cache: bool = True, cache_file: str = "slack_cache.json"):
        self.client = WebClient(token=bot_token)
        self.channels: Dict[str, Channel] = {}
        self.messages: List[Message] = []
        self.users: Dict[str, str] = {}  # user_id -> username mapping
        self.load_users = load_users
        self.use_cache = use_cache
        self.cache_manager = SlackCacheManager(cache_file) if use_cache else None
        
    def get_users(self) -> Dict[str, str]:
        """Get all users and create id -> username mapping"""
        try:
            response = self.client.users_list()
            users = {}
            for user in response.get("members", []):
                users[user["id"]] = user.get("real_name", user.get("display_name", user.get("name", "Unknown")))
            return users
        except SlackApiError as e:
            logger.error(f"Error fetching users: {e}")
            return {}
    
    def get_all_channels(self) -> List[Channel]:
        """Get all channels that the bot has access to"""
        try:
            response = self.client.conversations_list(
                exclude_archived=True,
                types="public_channel,private_channel"
            )
            
            channels = []
            for channel_data in response.get("channels", []):
                channel = Channel(
                    id=channel_data["id"],
                    name=channel_data["name"],
                    purpose=channel_data.get("purpose", {}).get("value", ""),
                    topic=channel_data.get("topic", {}).get("value", ""),
                    member_count=channel_data.get("num_members", 0),
                    is_private=channel_data.get("is_private", False),
                    is_archived=channel_data.get("is_archived", False),
                    is_member=channel_data.get("is_member", False),
                    created=channel_data.get("created", ""),
                    creator=channel_data.get("creator", "")
                )
                channels.append(channel)
                self.channels[channel.id] = channel
                
            return channels
        except SlackApiError as e:
            logger.error(f"Error fetching channels: {e}")
            return []
    
    def join_all_channels(self, channels: List[Channel]) -> List[str]:
        """Join all available channels"""
        joined_channels = []
        failed_channels = []
        already_member = []
        private_channels = []
        
        for channel in channels:
            # Check if bot is already a member
            if channel.is_member:
                already_member.append(channel.name)
                logger.debug(f"Already member of #{channel.name}, skipping")
                continue
            
            # Check if channel is private
            if channel.is_private:
                private_channels.append(channel.name)
                logger.debug(f"#{channel.name} is private, cannot join")
                continue
            
            # Try to join the channel
            try:
                logger.info(f"Attempting to join #{channel.name}...")
                self.client.conversations_join(channel=channel.id)
                joined_channels.append(channel.name)
                logger.info(f"✅ Successfully joined #{channel.name}")
                time.sleep(1)  # Rate limiting
            except SlackApiError as e:
                failed_channels.append(f"#{channel.name}: {e}")
                logger.warning(f"❌ Failed to join #{channel.name}: {e}")
        
        # Summary logging
        logger.info(f"Channel membership summary:")
        logger.info(f"  - Already member: {len(already_member)} channels")
        logger.info(f"  - Successfully joined: {len(joined_channels)} channels")
        logger.info(f"  - Private (cannot join): {len(private_channels)} channels")
        if failed_channels:
            logger.warning(f"  - Failed to join: {len(failed_channels)} channels")
            
        return joined_channels
    
    def extract_channel_messages(self, channel_id: str, limit: int = 1000, force_refresh: bool = False) -> List[Message]:
        """Extract all messages and threads from a specific channel with caching support"""
        messages = []
        thread_parents_found = 0
        channel_name = self.channels[channel_id].name if channel_id in self.channels else "unknown"
        
        # Get cached messages (we'll merge with new ones)
        cached_messages = []
        if self.use_cache and self.cache_manager and not force_refresh:
            cached_messages = self._get_cached_channel_messages(channel_id)
        
        try:
            # Determine if we need full fetch or incremental
            oldest_param = {}
            if self.use_cache and self.cache_manager and not force_refresh and cached_messages:
                last_message_ts = self.cache_manager.get_channel_last_message_ts(channel_id)
                if last_message_ts:
                    oldest_param["oldest"] = last_message_ts
                    logger.info(f"#{channel_name}: Fetching new messages since {last_message_ts}")
                else:
                    logger.info(f"#{channel_name}: Full fetch (no timestamp in cache)")
            else:
                logger.info(f"#{channel_name}: Full fetch (force refresh or no cache)")
            
            # Get channel history
            response = self.client.conversations_history(
                channel=channel_id,
                limit=limit,
                **oldest_param
            )
            
            new_messages = []
            latest_ts = None
            
            for msg_data in response.get("messages", []):
                message = self._parse_message(msg_data, channel_id, channel_name)
                if message:
                    new_messages.append(message)
                    
                    # Track latest timestamp
                    if not latest_ts or message.timestamp > latest_ts:
                        latest_ts = message.timestamp
                    
                    # If this is a thread parent, get all replies
                    if message.is_thread_parent:
                        thread_parents_found += 1
                        thread_messages = self._extract_thread_replies(channel_id, message.timestamp)
                        logger.debug(f"Found thread parent in #{channel_name} with {len(thread_messages)} replies")
                        new_messages.extend(thread_messages)
            
            # Determine final message list
            if oldest_param and new_messages:
                # Incremental update - merge with existing cache
                logger.info(f"#{channel_name}: Found {len(new_messages)} new messages")
                messages = cached_messages + new_messages
                
                # Update cache incrementally
                if self.use_cache and self.cache_manager:
                    self.cache_manager.merge_with_existing_messages([asdict(msg) for msg in new_messages])
                    self.cache_manager.update_channel_info(channel_id, channel_name, latest_ts)
                    
            elif oldest_param and not new_messages:
                # No new messages - use cache
                logger.info(f"#{channel_name}: No new messages, using cached data ({len(cached_messages)} messages)")
                messages = cached_messages
                
                # Update the last fetch time even if no new messages
                if self.use_cache and self.cache_manager:
                    self.cache_manager.update_channel_info(channel_id, channel_name, 
                                                         self.cache_manager.get_channel_last_message_ts(channel_id))
                    
            else:
                # Full fetch - replace cache
                logger.info(f"#{channel_name}: Full fetch returned {len(new_messages)} messages")
                messages = new_messages
                
                if self.use_cache and self.cache_manager:
                    # Clear existing messages for this channel from cache
                    self.cache_manager.cache_data["messages"] = [
                        msg for msg in self.cache_manager.cache_data["messages"]
                        if msg.get("channel_id") != channel_id
                    ]
                    self.cache_manager.add_messages([asdict(msg) for msg in new_messages])
                    self.cache_manager.update_channel_info(channel_id, channel_name, latest_ts)
                        
            if thread_parents_found > 0:
                logger.info(f"#{channel_name}: Found {thread_parents_found} thread parents")
                        
        except SlackApiError as e:
            logger.error(f"Error extracting messages from channel {channel_id}: {e}")
            # Fall back to cache if available
            if cached_messages:
                logger.info(f"#{channel_name}: Using cached data due to API error")
                return cached_messages
            
        return messages
    
    def _get_cached_channel_messages(self, channel_id: str) -> List[Message]:
        """Get cached messages for a specific channel"""
        if not self.use_cache or not self.cache_manager:
            return []
        
        cached_data = self.cache_manager.get_cached_messages(channel_id)
        messages = []
        
        for msg_data in cached_data:
            try:
                # Convert dict back to Message object
                message = Message(
                    id=msg_data.get("id", ""),
                    channel_id=msg_data.get("channel_id", ""),
                    channel_name=msg_data.get("channel_name", ""),
                    user_id=msg_data.get("user_id", ""),
                    username=msg_data.get("username", ""),
                    timestamp=msg_data.get("timestamp", ""),
                    text=msg_data.get("text", ""),
                    message_type=msg_data.get("message_type", "message"),
                    subtype=msg_data.get("subtype"),
                    thread_ts=msg_data.get("thread_ts"),
                    is_thread_parent=msg_data.get("is_thread_parent", False),
                    reply_count=msg_data.get("reply_count", 0),
                    reactions=msg_data.get("reactions", []),
                    attachments=msg_data.get("attachments", [])
                )
                messages.append(message)
            except Exception as e:
                logger.warning(f"Error converting cached message: {e}")
                continue
        
        return messages
    
    def _extract_thread_replies(self, channel_id: str, thread_ts: str) -> List[Message]:
        """Extract all replies from a thread"""
        replies = []
        
        try:
            response = self.client.conversations_replies(
                channel=channel_id,
                ts=thread_ts
            )
            
            channel_name = self.channels[channel_id].name if channel_id in self.channels else "unknown"
            
            for msg_data in response.get("messages", []):
                # Skip the parent message (already processed)
                if msg_data.get("ts") != thread_ts:
                    message = self._parse_message(msg_data, channel_id, channel_name)
                    if message:
                        replies.append(message)
                        
        except SlackApiError as e:
            logger.error(f"Error extracting thread replies: {e}")
            
        return replies
    
    def _parse_message(self, msg_data: Dict[str, Any], channel_id: str, channel_name: str) -> Optional[Message]:
        """Parse raw message data into Message object"""
        user_id = msg_data.get("user", "")
        if self.load_users:
            username = self.users.get(user_id, user_id) if user_id else "Unknown"
            username = username if username else "Unknown"
        else:
            username = f"user_{user_id[:8]}" if user_id else "Unknown"
        
        text = msg_data.get("text", "")
        if not text and msg_data.get("subtype") == "bot_message":
            text = msg_data.get("text", "")
        
        # Skip certain message types
        if msg_data.get("subtype") in ["channel_join", "channel_leave", "channel_archive"]:
            return None
            
        thread_ts = msg_data.get("thread_ts")
        ts = msg_data.get("ts", "")
        reply_count = msg_data.get("reply_count", 0)
        
        # A message is a thread parent if:
        # 1. It has thread_ts equal to its own ts, OR
        # 2. It has replies (reply_count > 0)
        is_thread_parent = bool((thread_ts and thread_ts == ts) or reply_count > 0)
        
        return Message(
            id=ts,
            channel_id=channel_id,
            channel_name=channel_name,
            user_id=user_id,
            username=username,
            timestamp=ts,
            text=text,
            message_type=msg_data.get("type", "message"),
            subtype=msg_data.get("subtype"),
            thread_ts=thread_ts,
            is_thread_parent=is_thread_parent,
            reply_count=reply_count,
            reactions=msg_data.get("reactions", []),
            attachments=msg_data.get("attachments", [])
        )
    
    def filter_messages(self, messages: List[Message], filters: Dict[str, Any]) -> List[Message]:
        """Filter messages based on various criteria"""
        filtered_messages = messages
        
        # Filter by date range
        if filters.get("start_date") or filters.get("end_date"):
            filtered_messages = self._filter_by_date(filtered_messages, filters)
        
        # Filter by channel
        if filters.get("channels"):
            channel_names = set(filters["channels"])
            filtered_messages = [msg for msg in filtered_messages if msg.channel_name in channel_names]
        
        # Filter by user
        if filters.get("users"):
            usernames = set(filters["users"])
            filtered_messages = [msg for msg in filtered_messages if msg.username in usernames]
        
        # Filter by text content
        if filters.get("keywords"):
            filtered_messages = self._filter_by_keywords(filtered_messages, filters["keywords"])
        
        # Remove bot messages if specified
        if filters.get("exclude_bots", False):
            filtered_messages = [msg for msg in filtered_messages if msg.subtype != "bot_message"]
        
        # Remove empty messages
        filtered_messages = [msg for msg in filtered_messages if msg.text.strip()]
        
        return filtered_messages
    
    def _filter_by_date(self, messages: List[Message], filters: Dict[str, Any]) -> List[Message]:
        """Filter messages by date range"""
        start_date = filters.get("start_date")
        end_date = filters.get("end_date")
        
        filtered = []
        for msg in messages:
            msg_date = datetime.fromtimestamp(float(msg.timestamp))
            
            if start_date and msg_date < start_date:
                continue
            if end_date and msg_date > end_date:
                continue
                
            filtered.append(msg)
        
        return filtered
    
    def _filter_by_keywords(self, messages: List[Message], keywords: List[str]) -> List[Message]:
        """Filter messages containing specified keywords"""
        filtered = []
        
        for msg in messages:
            text_lower = msg.text.lower()
            if any(keyword.lower() in text_lower for keyword in keywords):
                filtered.append(msg)
        
        return filtered
    
    def search_messages(self, query: str, messages: List[Message], limit: int = 50) -> List[Message]:
        """Search messages using fuzzy matching"""
        if not query:
            return messages[:limit]
        
        # Create searchable text for each message
        searchable_messages = []
        for msg in messages:
            searchable_text = f"{msg.text} {msg.channel_name} {msg.username}"
            searchable_messages.append((searchable_text, msg))
        
        # Use fuzzy matching to find relevant messages
        results = process.extract(
            query,
            [text for text, _ in searchable_messages],
            limit=limit * 2,  # Get more results to filter
            scorer=fuzz.partial_ratio
        )
        
        # Filter by minimum score and return messages
        relevant_messages = []
        for match, score in results:
            if score >= 60:  # Minimum relevance threshold
                # Find the corresponding message
                for text, msg in searchable_messages:
                    if text == match:
                        relevant_messages.append(msg)
                        break
        
        return relevant_messages[:limit]
    
    def generate_llm_context(self, messages: List[Message], query: str = "", minimal: bool = True) -> str:
        """Generate structured context for LLM processing with deduplication"""
        
        # Step 1: Remove duplicates based on message ID and text content
        seen_messages = set()
        unique_messages = []
        
        for msg in messages:
            # Create a unique key based on message ID, text content, and timestamp
            # This handles cases where same message might appear multiple times
            unique_key = (msg.id, msg.text.strip(), msg.timestamp, msg.channel_id)
            
            if unique_key not in seen_messages:
                seen_messages.add(unique_key)
                unique_messages.append(msg)
            else:
                logger.debug(f"Removing duplicate message: {msg.text[:50]}...")
        
        logger.info(f"Removed {len(messages) - len(unique_messages)} duplicate messages")
        
        context = {
            "search_query": query,
            "total_messages": len(unique_messages),
            "channels_included": list(set(msg.channel_name for msg in unique_messages)),
            "conversations": []
        }
        
        if not minimal:
            context["date_range"] = self._get_date_range(unique_messages)
        
        # Group messages by channel and thread
        channel_groups: Dict[str, Dict[str, Any]] = {}
        for msg in unique_messages:
            channel_key = msg.channel_name
            if channel_key not in channel_groups:
                channel_groups[channel_key] = {"standalone": [], "threads": {}}
            
            if msg.thread_ts and msg.thread_ts != msg.timestamp:
                # This is a thread reply
                if msg.thread_ts not in channel_groups[channel_key]["threads"]:
                    channel_groups[channel_key]["threads"][msg.thread_ts] = []
                channel_groups[channel_key]["threads"][msg.thread_ts].append(msg)
            else:
                # This is either a standalone message or thread parent
                channel_groups[channel_key]["standalone"].append(msg)
        
        # Format conversations for LLM
        for channel_name, channel_data in channel_groups.items():
            # Get channel_id from any message in this channel group
            channel_id = None
            if channel_data["standalone"]:
                channel_id = channel_data["standalone"][0].channel_id
            elif channel_data["threads"]:
                # Get from first thread message
                first_thread_messages = list(channel_data["threads"].values())[0]
                if first_thread_messages:
                    channel_id = first_thread_messages[0].channel_id
            
            channel_context = {
                "channel": channel_name,
                "channel_id": channel_id,
                "standalone_messages": [],
                "threads": []
            }
            
            # Add standalone messages (remove duplicates within channel)
            standalone_texts = set()
            for msg in sorted(channel_data["standalone"], key=lambda x: x.timestamp):
                # Additional deduplication within the same channel based on text content
                if msg.text.strip() not in standalone_texts:
                    standalone_texts.add(msg.text.strip())
                    msg_data = {
                        "text": msg.text,
                        "timestamp": f"p{msg.timestamp.replace('.', '')}"
                    }
                    if not minimal:
                        msg_data.update({
                            "user": msg.username,
                            "has_thread": msg.is_thread_parent
                        })
                    channel_context["standalone_messages"].append(msg_data)
            
            # Add threads (remove duplicates within threads)
            for thread_ts, thread_messages in channel_data["threads"].items():
                thread_context = {
                    "messages": []
                }
                if not minimal:
                    thread_context["thread_id"] = thread_ts
                
                thread_texts = set()
                for msg in sorted(thread_messages, key=lambda x: x.timestamp):
                    # Additional deduplication within the same thread based on text content
                    if msg.text.strip() not in thread_texts:
                        thread_texts.add(msg.text.strip())
                        msg_data = {
                            "text": msg.text,
                            "timestamp": f"p{msg.timestamp.replace('.', '')}"
                        }
                        if not minimal:
                            msg_data.update({
                                "user": msg.username
                            })
                        thread_context["messages"].append(msg_data)
                
                # Only add thread if it has messages after deduplication
                if thread_context["messages"]:
                    channel_context["threads"].append(thread_context)
            
            # Only add channel if it has messages after deduplication
            if channel_context["standalone_messages"] or channel_context["threads"]:
                context["conversations"].append(channel_context)
        
        # Update total count after all deduplication
        total_final_messages = sum(
            len(conv["standalone_messages"]) + 
            sum(len(thread["messages"]) for thread in conv["threads"])
            for conv in context["conversations"]
        )
        context["total_messages"] = total_final_messages
        
        return json.dumps(context, indent=2)
    
    def _get_date_range(self, messages: List[Message]) -> Dict[str, str]:
        """Get the date range of messages"""
        if not messages:
            return {"start": "", "end": ""}
        
        timestamps = [float(msg.timestamp) for msg in messages]
        start_time = min(timestamps)
        end_time = max(timestamps)
        
        return {
            "start": self._format_timestamp(str(start_time)),
            "end": self._format_timestamp(str(end_time))
        }
    
    def _format_timestamp(self, timestamp: str) -> str:
        """Format timestamp for human readability"""
        return datetime.fromtimestamp(float(timestamp)).strftime("%Y-%m-%d %H:%M:%S")
    
    def get_channel_summary(self, messages: List[Message]) -> Dict[str, Any]:
        """Generate a summary of channel activity"""
        if not messages:
            return {}
        
        channel_stats: Dict[str, Any] = {}
        user_stats: Dict[str, int] = {}
        
        for msg in messages:
            # Channel stats
            if msg.channel_name not in channel_stats:
                channel_stats[msg.channel_name] = {
                    "message_count": 0,
                    "thread_count": 0,
                    "users": set()
                }
            
            channel_stats[msg.channel_name]["message_count"] += 1
            channel_stats[msg.channel_name]["users"].add(msg.username)
            
            if msg.is_thread_parent:
                channel_stats[msg.channel_name]["thread_count"] += 1
            
            # User stats
            if msg.username not in user_stats:
                user_stats[msg.username] = 0
            user_stats[msg.username] += 1
        
        # Convert sets to lists for JSON serialization
        for channel in channel_stats:
            channel_stats[channel]["users"] = list(channel_stats[channel]["users"])
            channel_stats[channel]["user_count"] = len(channel_stats[channel]["users"])
        
        return {
            "channel_stats": channel_stats,
            "user_stats": dict(sorted(user_stats.items(), key=lambda x: x[1], reverse=True)[:10]),
            "total_messages": len(messages),
            "unique_users": len(user_stats),
            "unique_channels": len(channel_stats)
        }

def main():
    """Main function to demonstrate the SlackDataExtractor with caching"""
    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        logger.error("SLACK_BOT_TOKEN environment variable not set")
        return
    
    # Add cache-only mode option
    cache_only = os.environ.get("CACHE_ONLY", "false").lower() == "true"
    
    extractor = SlackDataExtractor(token, load_users=False, use_cache=True)  # Enable caching
    
    # Show cache stats
    if extractor.use_cache and extractor.cache_manager:
        cache_stats = extractor.cache_manager.get_cache_stats()
        logger.info(f"Cache stats: {cache_stats['total_messages']} messages, {cache_stats['total_channels']} channels")
        if cache_stats['last_update']:
            logger.info(f"Cache last updated: {cache_stats['last_update']}")
    
    if cache_only:
        logger.info("CACHE-ONLY MODE: Working entirely from cache, no API calls")
        # Get all messages from cache
        all_messages = []
        if extractor.use_cache and extractor.cache_manager:
            cached_messages = extractor.cache_manager.get_cached_messages()
            for msg_data in cached_messages:
                try:
                    message = Message(
                        id=msg_data.get("id", ""),
                        channel_id=msg_data.get("channel_id", ""),
                        channel_name=msg_data.get("channel_name", ""),
                        user_id=msg_data.get("user_id", ""),
                        username=msg_data.get("username", ""),
                        timestamp=msg_data.get("timestamp", ""),
                        text=msg_data.get("text", ""),
                        message_type=msg_data.get("message_type", "message"),
                        subtype=msg_data.get("subtype"),
                        thread_ts=msg_data.get("thread_ts"),
                        is_thread_parent=msg_data.get("is_thread_parent", False),
                        reply_count=msg_data.get("reply_count", 0),
                        reactions=msg_data.get("reactions", []),
                        attachments=msg_data.get("attachments", [])
                    )
                    all_messages.append(message)
                except Exception as e:
                    logger.warning(f"Error converting cached message: {e}")
                    continue
        
        logger.info(f"Loaded {len(all_messages)} messages from cache")
        
    else:
        # Regular mode with API calls
        # Step 1: Get users mapping (optional)
        if extractor.load_users:
            logger.info("Fetching users...")
            extractor.users = extractor.get_users()
        
        # Step 2: Get all channels (use cache if available)
        logger.info("Fetching channels...")
        channels = extractor.get_all_channels()
        logger.info(f"Found {len(channels)} channels")
        
        # Step 3: Join all available channels (with rate limiting)
        logger.info("Joining channels...")
        joined = extractor.join_all_channels(channels)
        logger.info(f"Joined {len(joined)} new channels")
        time.sleep(2)  # Wait after joining channels
    
    # Step 4: Extract messages from all channels (ULTRA-CONSERVATIVE with smart caching)
    logger.info("Extracting messages from all channels...")
    all_messages = []
    
    # Get all channels that need processing
    channels_to_process = []
    for channel in channels:
        if channel.is_member or channel.name in [c.replace('#', '') for c in joined]:
            channels_to_process.append(channel)
    
    logger.info(f"Processing {len(channels_to_process)} channels with ultra-conservative rate limiting")
    
    # Ultra-conservative settings
    base_delay = 10.0  # 10 seconds between channels
    max_delay = 120.0  # Maximum delay of 2 minutes
    current_delay = base_delay
    
    # Process channels ONE AT A TIME with long delays
    for i, channel in enumerate(channels_to_process):
        logger.info(f"Processing channel {i+1}/{len(channels_to_process)}: #{channel.name}")
        
        retry_count = 0
        max_retries = 2  # Reduced retries to avoid hitting limits
        success = False
        
        while retry_count <= max_retries and not success:
            try:
                # Check if we can use cache (very recent data)
                if extractor.use_cache and extractor.cache_manager:
                    # Use cache if it's very recent (last 10 minutes)
                    last_fetch = extractor.cache_manager.get_channel_last_fetch(channel.id)
                    if last_fetch:
                        from datetime import datetime, timedelta
                        last_fetch_time = datetime.fromisoformat(last_fetch)
                        if datetime.now() - last_fetch_time < timedelta(minutes=10):
                            logger.info(f"#{channel.name}: Using very recent cache (< 10 min old)")
                            cached_messages = extractor._get_cached_channel_messages(channel.id)
                            all_messages.extend(cached_messages)
                            success = True
                            continue
                
                logger.info(f"#{channel.name}: Fetching from API (attempt {retry_count + 1})")
                messages = extractor.extract_channel_messages(channel.id)
                all_messages.extend(messages)
                success = True
                
                # Reset delay on success
                current_delay = base_delay
                logger.info(f"#{channel.name}: Successfully processed, waiting {current_delay} seconds...")
                
            except SlackApiError as e:
                if e.response["error"] == "ratelimited":
                    retry_count += 1
                    
                    # Get retry-after header if available
                    retry_after = e.response.get("headers", {}).get("retry-after", current_delay)
                    if isinstance(retry_after, str):
                        try:
                            retry_after = float(retry_after)
                        except ValueError:
                            retry_after = current_delay
                    
                    # Use much longer wait time
                    wait_time = max(retry_after * 2, current_delay * 2)  # Double the suggested wait time
                    logger.warning(f"#{channel.name}: Rate limited! Attempt {retry_count}/{max_retries}, waiting {wait_time:.1f} seconds...")
                    time.sleep(wait_time)
                    
                    # Exponential backoff
                    current_delay = min(current_delay * 2, max_delay)
                    
                else:
                    logger.error(f"#{channel.name}: API error: {e}")
                    break
                    
            except Exception as e:
                logger.error(f"#{channel.name}: Unexpected error: {e}")
                break
        
        # If we failed to get new data, use cache
        if not success:
            logger.warning(f"#{channel.name}: Failed to fetch new data, using cached data")
            if extractor.use_cache and extractor.cache_manager:
                cached_messages = extractor._get_cached_channel_messages(channel.id)
                all_messages.extend(cached_messages)
                logger.info(f"#{channel.name}: Using cached data ({len(cached_messages)} messages)")
        
        # Long delay between channels (except for the last one)
        if i < len(channels_to_process) - 1:
            delay = max(current_delay, 15.0)  # At least 15 seconds between channels
            logger.info(f"Waiting {delay:.1f} seconds before next channel...")
            time.sleep(delay)
    
    logger.info(f"Extracted {len(all_messages)} total messages")
    
    # Step 5: Save cache
    if extractor.use_cache and extractor.cache_manager:
        extractor.cache_manager.save_cache()
        logger.info("Cache saved successfully")
        
        # Step 5.5: Reload all messages from cache to ensure we have the most complete data
        logger.info("Reloading all messages from updated cache...")
        all_messages = []
        cached_messages = extractor.cache_manager.get_cached_messages()
        for msg_data in cached_messages:
            try:
                message = Message(
                    id=msg_data.get("id", ""),
                    channel_id=msg_data.get("channel_id", ""),
                    channel_name=msg_data.get("channel_name", ""),
                    user_id=msg_data.get("user_id", ""),
                    username=msg_data.get("username", ""),
                    timestamp=msg_data.get("timestamp", ""),
                    text=msg_data.get("text", ""),
                    message_type=msg_data.get("message_type", "message"),
                    subtype=msg_data.get("subtype"),
                    thread_ts=msg_data.get("thread_ts"),
                    is_thread_parent=msg_data.get("is_thread_parent", False),
                    reply_count=msg_data.get("reply_count", 0),
                    reactions=msg_data.get("reactions", []),
                    attachments=msg_data.get("attachments", [])
                )
                all_messages.append(message)
            except Exception as e:
                logger.warning(f"Error converting cached message: {e}")
                continue
        
        logger.info(f"Reloaded {len(all_messages)} messages from cache")
    
    # Step 6: Filter messages (example filters)
    filters = {
        "exclude_bots": True,
        "start_date": datetime.now() - timedelta(days=30),  # Last 30 days
        # "keywords": ["bug", "feature", "help"],  # Uncomment to filter by keywords
        # "channels": ["general", "development"],  # Uncomment to filter by channels
    }
    
    filtered_messages = extractor.filter_messages(all_messages, filters)
    logger.info(f"Filtered to {len(filtered_messages)} relevant messages")
    
    # Step 7: Generate LLM context from latest cache data
    logger.info("Generating LLM context from latest cache data...")
    llm_context = extractor.generate_llm_context(filtered_messages, "", minimal=True)
    
    # Step 8: Save results
    with open("slack_data.json", "w") as f:
        f.write(llm_context)
    
    logger.info("Data extraction complete!")
    logger.info(f"LLM context saved to slack_data.json (from latest cache)")
    
    # Show final cache stats
    if extractor.use_cache and extractor.cache_manager:
        final_stats = extractor.cache_manager.get_cache_stats()
        logger.info(f"Final cache: {final_stats['total_messages']} messages, cache size: {final_stats['cache_file_size']} bytes")


if __name__ == "__main__":
    main()