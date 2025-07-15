#!/usr/bin/env python3
"""
Cache Manager for Slack Data - Handles caching of channels and messages to avoid redundant API calls
"""

import json
import os
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import asdict
import logging

logger = logging.getLogger(__name__)

class SlackCacheManager:
    """Manages caching of Slack data to improve performance on subsequent runs"""
    
    def __init__(self, cache_file: str = "slack_cache.json"):
        self.cache_file = cache_file
        self.cache_data = {
            "last_update": None,
            "channels": {},  # channel_id -> channel info and last fetch time
            "messages": [],  # all messages
            "metadata": {
                "version": "1.0",
                "created": datetime.now().isoformat()
            }
        }
        self.load_cache()
    
    def load_cache(self) -> None:
        """Load existing cache from file"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    loaded_data = json.load(f)
                    # Merge with default structure to handle version changes
                    self.cache_data.update(loaded_data)
                logger.info(f"Loaded cache from {self.cache_file}")
                logger.info(f"Cache contains {len(self.cache_data['channels'])} channels and {len(self.cache_data['messages'])} messages")
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Could not load cache file {self.cache_file}: {e}")
                logger.info("Starting with empty cache")
        else:
            logger.info("No cache file found, starting with empty cache")
    
    def save_cache(self) -> None:
        """Save current cache to file"""
        try:
            self.cache_data["last_update"] = datetime.now().isoformat()
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache_data, f, indent=2)
            logger.info(f"Cache saved to {self.cache_file}")
        except IOError as e:
            logger.error(f"Could not save cache to {self.cache_file}: {e}")
    
    def get_channel_last_fetch(self, channel_id: str) -> Optional[str]:
        """Get the last fetch timestamp for a channel"""
        channel_info = self.cache_data["channels"].get(channel_id)
        if channel_info:
            return channel_info.get("last_fetch")
        return None
    
    def get_channel_last_message_ts(self, channel_id: str) -> Optional[str]:
        """Get the timestamp of the last message in a channel"""
        channel_info = self.cache_data["channels"].get(channel_id)
        if channel_info:
            return channel_info.get("last_message_ts")
        return None
    
    def update_channel_info(self, channel_id: str, channel_name: str, last_message_ts: Optional[str] = None) -> None:
        """Update channel information in cache"""
        if channel_id not in self.cache_data["channels"]:
            self.cache_data["channels"][channel_id] = {
                "name": channel_name,
                "last_fetch": datetime.now().isoformat(),
                "last_message_ts": last_message_ts
            }
        else:
            self.cache_data["channels"][channel_id]["last_fetch"] = datetime.now().isoformat()
            if last_message_ts:
                self.cache_data["channels"][channel_id]["last_message_ts"] = last_message_ts
    
    def add_messages(self, messages: List[Dict[str, Any]]) -> None:
        """Add new messages to cache"""
        # Messages should already be dictionaries
        self.cache_data["messages"].extend(messages)
        logger.info(f"Added {len(messages)} messages to cache")
    
    def get_cached_messages(self, channel_id: Optional[str] = None, since_ts: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get cached messages, optionally filtered by channel and timestamp"""
        messages = self.cache_data["messages"]
        
        if channel_id:
            messages = [msg for msg in messages if msg.get("channel_id") == channel_id]
        
        if since_ts:
            messages = [msg for msg in messages if msg.get("timestamp", "0") > since_ts]
        
        return messages
    
    def get_all_cached_messages(self) -> List[Dict[str, Any]]:
        """Get all cached messages"""
        return self.cache_data["messages"]
    
    def remove_old_messages(self, older_than_days: int = 90) -> None:
        """Remove messages older than specified days to keep cache size manageable"""
        cutoff_timestamp = str(time.time() - (older_than_days * 24 * 60 * 60))
        original_count = len(self.cache_data["messages"])
        
        self.cache_data["messages"] = [
            msg for msg in self.cache_data["messages"]
            if msg.get("timestamp", "0") > cutoff_timestamp
        ]
        
        removed_count = original_count - len(self.cache_data["messages"])
        if removed_count > 0:
            logger.info(f"Removed {removed_count} messages older than {older_than_days} days from cache")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        channel_stats = {}
        for msg in self.cache_data["messages"]:
            channel_name = msg.get("channel_name", "unknown")
            channel_stats[channel_name] = channel_stats.get(channel_name, 0) + 1
        
        return {
            "total_messages": len(self.cache_data["messages"]),
            "total_channels": len(self.cache_data["channels"]),
            "messages_per_channel": channel_stats,
            "cache_file_size": os.path.getsize(self.cache_file) if os.path.exists(self.cache_file) else 0,
            "last_update": self.cache_data.get("last_update")
        }
    
    def clear_cache(self) -> None:
        """Clear all cache data"""
        self.cache_data = {
            "last_update": None,
            "channels": {},
            "messages": [],
            "metadata": {
                "version": "1.0",
                "created": datetime.now().isoformat()
            }
        }
        if os.path.exists(self.cache_file):
            os.remove(self.cache_file)
        logger.info("Cache cleared")
    
    def should_fetch_channel(self, channel_id: str, max_age_hours: int = 1) -> bool:
        """Determine if a channel should be fetched based on last fetch time"""
        last_fetch = self.get_channel_last_fetch(channel_id)
        if not last_fetch:
            return True
        
        try:
            last_fetch_time = datetime.fromisoformat(last_fetch)
            age_hours = (datetime.now() - last_fetch_time).total_seconds() / 3600
            return age_hours > max_age_hours
        except (ValueError, TypeError):
            return True
    
    def merge_with_existing_messages(self, new_messages: List[Dict[str, Any]]) -> None:
        """Merge new messages with existing cache, avoiding duplicates"""
        existing_ids = {msg.get("id") for msg in self.cache_data["messages"]}
        unique_new_messages = [msg for msg in new_messages if msg.get("id") not in existing_ids]
        
        if unique_new_messages:
            self.add_messages(unique_new_messages)
            logger.info(f"Added {len(unique_new_messages)} new unique messages to cache")
        else:
            logger.info("No new messages to add to cache") 