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
    
    def __init__(self, bot_token: str, load_users: bool = False):
        self.client = WebClient(token=bot_token)
        self.channels: Dict[str, Channel] = {}
        self.messages: List[Message] = []
        self.users: Dict[str, str] = {}  # user_id -> username mapping
        self.load_users = load_users
        
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
    
    def extract_channel_messages(self, channel_id: str, limit: int = 1000) -> List[Message]:
        """Extract all messages and threads from a specific channel"""
        messages = []
        thread_parents_found = 0
        
        try:
            # Get channel history
            response = self.client.conversations_history(
                channel=channel_id,
                limit=limit
            )
            
            channel_name = self.channels[channel_id].name if channel_id in self.channels else "unknown"
            
            for msg_data in response.get("messages", []):
                message = self._parse_message(msg_data, channel_id, channel_name)
                if message:
                    messages.append(message)
                    
                    # If this is a thread parent, get all replies
                    if message.is_thread_parent:
                        thread_parents_found += 1
                        thread_messages = self._extract_thread_replies(channel_id, message.timestamp)
                        logger.debug(f"Found thread parent in #{channel_name} with {len(thread_messages)} replies")
                        messages.extend(thread_messages)
                        
            if thread_parents_found > 0:
                logger.info(f"#{channel_name}: Found {thread_parents_found} thread parents")
                        
        except SlackApiError as e:
            logger.error(f"Error extracting messages from channel {channel_id}: {e}")
            
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
        """Generate structured context for LLM processing"""
        context = {
            "search_query": query,
            "total_messages": len(messages),
            "channels_included": list(set(msg.channel_name for msg in messages)),
            "conversations": []
        }
        
        if not minimal:
            context["date_range"] = self._get_date_range(messages)
        
        # Group messages by channel and thread
        channel_groups: Dict[str, Dict[str, Any]] = {}
        for msg in messages:
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
            channel_context = {
                "channel": channel_name,
                "standalone_messages": [],
                "threads": []
            }
            
            # Add standalone messages
            for msg in sorted(channel_data["standalone"], key=lambda x: x.timestamp):
                msg_data = {"text": msg.text}
                if not minimal:
                    msg_data.update({
                        "user": msg.username,
                        "timestamp": self._format_timestamp(msg.timestamp),
                        "has_thread": msg.is_thread_parent
                    })
                channel_context["standalone_messages"].append(msg_data)
            
            # Add threads
            for thread_ts, thread_messages in channel_data["threads"].items():
                thread_context = {
                    "messages": []
                }
                if not minimal:
                    thread_context["thread_id"] = thread_ts
                
                for msg in sorted(thread_messages, key=lambda x: x.timestamp):
                    msg_data = {"text": msg.text}
                    if not minimal:
                        msg_data.update({
                            "user": msg.username,
                            "timestamp": self._format_timestamp(msg.timestamp)
                        })
                    thread_context["messages"].append(msg_data)
                
                channel_context["threads"].append(thread_context)
            
            context["conversations"].append(channel_context)
        
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
    """Main function to demonstrate the SlackDataExtractor"""
    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        logger.error("SLACK_BOT_TOKEN environment variable not set")
        return
    
    extractor = SlackDataExtractor(token, load_users=False)  # Don't load users by default
    
    # Step 1: Get users mapping (optional)
    if extractor.load_users:
        logger.info("Fetching users...")
        extractor.users = extractor.get_users()
    
    # Step 2: Get all channels
    logger.info("Fetching channels...")
    channels = extractor.get_all_channels()
    logger.info(f"Found {len(channels)} channels")
    
    # Step 3: Join all available channels
    logger.info("Joining channels...")
    joined = extractor.join_all_channels(channels)
    logger.info(f"Joined {len(joined)} new channels")
    
    # Step 4: Extract messages from all channels
    logger.info("Extracting messages from all channels...")
    all_messages = []
    
    for channel in channels:
        if channel.is_member or channel.name in [c.replace('#', '') for c in joined]:
            logger.info(f"Processing channel: #{channel.name}")
            messages = extractor.extract_channel_messages(channel.id)
            all_messages.extend(messages)
            time.sleep(1)  # Rate limiting
    
    logger.info(f"Extracted {len(all_messages)} total messages")
    
    # Step 5: Filter messages (example filters)
    filters = {
        "exclude_bots": True,
        "start_date": datetime.now() - timedelta(days=30),  # Last 30 days
        # "keywords": ["bug", "feature", "help"],  # Uncomment to filter by keywords
        # "channels": ["general", "development"],  # Uncomment to filter by channels
    }
    
    filtered_messages = extractor.filter_messages(all_messages, filters)
    logger.info(f"Filtered to {len(filtered_messages)} relevant messages")
    
    # Step 6: Generate LLM context
    logger.info("Generating LLM context...")
    llm_context = extractor.generate_llm_context(filtered_messages, "", minimal=True)
    
    # Step 7: Save results
    with open("slack_data.json", "w") as f:
        f.write(llm_context)
    
    logger.info("Data extraction complete!")
    logger.info(f"LLM context saved to slack_data.json")
    
    # Example search
    search_query = "deployment issue"
    search_results = extractor.search_messages(search_query, filtered_messages, limit=20)
    logger.info(f"Search for '{search_query}' returned {len(search_results)} results")

if __name__ == "__main__":
    main()