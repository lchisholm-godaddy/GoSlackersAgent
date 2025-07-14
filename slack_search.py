#!/usr/bin/env python3
"""
Slack Search Interface - A user-friendly API for searching Slack channels and generating LLM context
"""

import os
import json
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from getchannels import SlackDataExtractor, Message, Channel

class SlackSearchInterface:
    """High-level interface for Slack search operations"""
    
    def __init__(self, bot_token: Optional[str] = None):
        """Initialize the search interface"""
        self.bot_token = bot_token or os.environ.get("SLACK_BOT_TOKEN")
        if not self.bot_token:
            raise ValueError("SLACK_BOT_TOKEN environment variable not set")
        
        self.extractor = SlackDataExtractor(self.bot_token)
        self.channels: List[Channel] = []
        self.all_messages: List[Message] = []
        self.is_data_loaded = False
    
    def initialize(self, join_channels: bool = True, days_back: int = 30):
        """Initialize the search interface by loading data from all channels"""
        print("🔄 Initializing Slack data...")
        
        # Load users
        print("📥 Fetching users...")
        self.extractor.users = self.extractor.get_users()
        
        # Load channels
        print("📥 Fetching channels...")
        self.channels = self.extractor.get_all_channels()
        print(f"Found {len(self.channels)} channels")
        
        # Join channels if requested
        if join_channels:
            print("🤝 Joining new channels...")
            joined = self.extractor.join_all_channels(self.channels)
            print(f"Joined {len(joined)} new channels")
        
        # Load messages from all channels
        print("📥 Loading messages from all channels...")
        self.all_messages = []
        
        for channel in self.channels:
            if channel.is_member or join_channels:
                print(f"Processing #{channel.name}...")
                messages = self.extractor.extract_channel_messages(channel.id)
                self.all_messages.extend(messages)
        
        # Filter by date range
        cutoff_date = datetime.now() - timedelta(days=days_back)
        self.all_messages = self.extractor.filter_messages(
            self.all_messages, 
            {"start_date": cutoff_date, "exclude_bots": True}
        )
        
        print(f"✅ Loaded {len(self.all_messages)} messages from last {days_back} days")
        self.is_data_loaded = True
    
    def search(self, query: str, limit: int = 20, channel_filter: Optional[List[str]] = None) -> Dict[str, Any]:
        """Search for messages matching the query"""
        if not self.is_data_loaded:
            raise RuntimeError("Data not loaded. Call initialize() first.")
        
        # Apply channel filter if provided
        messages_to_search = self.all_messages
        if channel_filter:
            messages_to_search = [msg for msg in self.all_messages if msg.channel_name in channel_filter]
        
        # Perform search
        results = self.extractor.search_messages(query, messages_to_search, limit)
        
        # Generate response
        return {
            "query": query,
            "total_results": len(results),
            "channels_searched": len(set(msg.channel_name for msg in messages_to_search)),
            "channels_with_results": len(set(msg.channel_name for msg in results)),
            "results": [self._format_message_result(msg) for msg in results],
            "channel_summary": self._get_channel_breakdown(results)
        }
    
    def search_by_keywords(self, keywords: List[str], limit: int = 20) -> Dict[str, Any]:
        """Search for messages containing specific keywords"""
        if not self.is_data_loaded:
            raise RuntimeError("Data not loaded. Call initialize() first.")
        
        filtered_messages = self.extractor.filter_messages(
            self.all_messages, 
            {"keywords": keywords}
        )
        
        return {
            "keywords": keywords,
            "total_results": len(filtered_messages),
            "results": [self._format_message_result(msg) for msg in filtered_messages[:limit]],
            "channel_summary": self._get_channel_breakdown(filtered_messages)
        }
    
    def search_by_channel(self, channel_names: List[str], query: Optional[str] = None, limit: int = 50) -> Dict[str, Any]:
        """Search within specific channels"""
        if not self.is_data_loaded:
            raise RuntimeError("Data not loaded. Call initialize() first.")
        
        channel_messages = [msg for msg in self.all_messages if msg.channel_name in channel_names]
        
        if query:
            results = self.extractor.search_messages(query, channel_messages, limit)
        else:
            results = channel_messages[:limit]
        
        return {
            "channels": channel_names,
            "query": query,
            "total_results": len(results),
            "results": [self._format_message_result(msg) for msg in results],
            "channel_summary": self._get_channel_breakdown(results)
        }
    
    def search_by_user(self, usernames: List[str], query: Optional[str] = None, limit: int = 50) -> Dict[str, Any]:
        """Search messages from specific users"""
        if not self.is_data_loaded:
            raise RuntimeError("Data not loaded. Call initialize() first.")
        
        user_messages = [msg for msg in self.all_messages if msg.username in usernames]
        
        if query:
            results = self.extractor.search_messages(query, user_messages, limit)
        else:
            results = user_messages[:limit]
        
        return {
            "users": usernames,
            "query": query,
            "total_results": len(results),
            "results": [self._format_message_result(msg) for msg in results],
            "channel_summary": self._get_channel_breakdown(results)
        }
    
    def get_trending_topics(self, limit: int = 10) -> Dict[str, Any]:
        """Get trending topics and frequently mentioned keywords"""
        if not self.is_data_loaded:
            raise RuntimeError("Data not loaded. Call initialize() first.")
        
        # Simple word frequency analysis
        word_counts: Dict[str, int] = {}
        for msg in self.all_messages:
            words = msg.text.lower().split()
            for word in words:
                if len(word) > 3 and word.isalpha():  # Filter out short words and non-alphabetic
                    word_counts[word] = word_counts.get(word, 0) + 1
        
        # Get top words
        trending = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:limit]
        
        return {
            "trending_keywords": [{"word": word, "count": count} for word, count in trending],
            "total_words_analyzed": len(word_counts),
            "message_count": len(self.all_messages)
        }
    
    def get_channel_activity(self) -> Dict[str, Any]:
        """Get channel activity statistics"""
        if not self.is_data_loaded:
            raise RuntimeError("Data not loaded. Call initialize() first.")
        
        return self.extractor.get_channel_summary(self.all_messages)
    
    def generate_llm_context(self, search_results: List[Message], query: str = "") -> str:
        """Generate LLM-ready context from search results"""
        return self.extractor.generate_llm_context(search_results, query)
    
    def find_relevant_channels(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Find channels most relevant to a query"""
        if not self.is_data_loaded:
            raise RuntimeError("Data not loaded. Call initialize() first.")
        
        # Search across all channels
        search_results = self.extractor.search_messages(query, self.all_messages, limit * 10)
        
        # Count results per channel
        channel_relevance: Dict[str, int] = {}
        for msg in search_results:
            channel_relevance[msg.channel_name] = channel_relevance.get(msg.channel_name, 0) + 1
        
        # Get top channels
        top_channels = sorted(channel_relevance.items(), key=lambda x: x[1], reverse=True)[:limit]
        
        # Format results
        relevant_channels = []
        for channel_name, message_count in top_channels:
            # Find channel metadata
            channel_info = next((ch for ch in self.channels if ch.name == channel_name), None)
            relevant_channels.append({
                "channel": channel_name,
                "relevant_messages": message_count,
                "purpose": channel_info.purpose if channel_info else "",
                "topic": channel_info.topic if channel_info else "",
                "member_count": channel_info.member_count if channel_info else 0
            })
        
        return relevant_channels
    
    def _format_message_result(self, message: Message) -> Dict[str, Any]:
        """Format a message for search results"""
        return {
            "channel": message.channel_name,
            "user": message.username,
            "timestamp": self.extractor._format_timestamp(message.timestamp),
            "text": message.text,
            "is_thread_parent": message.is_thread_parent,
            "reply_count": message.reply_count,
            "message_id": message.id
        }
    
    def _get_channel_breakdown(self, messages: List[Message]) -> Dict[str, int]:
        """Get a breakdown of messages by channel"""
        breakdown: Dict[str, int] = {}
        for msg in messages:
            breakdown[msg.channel_name] = breakdown.get(msg.channel_name, 0) + 1
        return dict(sorted(breakdown.items(), key=lambda x: x[1], reverse=True))


def main():
    """Command-line interface for Slack search"""
    parser = argparse.ArgumentParser(description="Search Slack channels and generate LLM context")
    parser.add_argument("--query", "-q", help="Search query")
    parser.add_argument("--channels", "-c", nargs="+", help="Specific channels to search")
    parser.add_argument("--users", "-u", nargs="+", help="Specific users to search")
    parser.add_argument("--keywords", "-k", nargs="+", help="Keywords to search for")
    parser.add_argument("--limit", "-l", type=int, default=20, help="Number of results to return")
    parser.add_argument("--days", "-d", type=int, default=30, help="Number of days back to search")
    parser.add_argument("--no-join", action="store_true", help="Don't join new channels")
    parser.add_argument("--trending", action="store_true", help="Show trending topics")
    parser.add_argument("--activity", action="store_true", help="Show channel activity")
    parser.add_argument("--relevant-channels", action="store_true", help="Find channels relevant to query")
    parser.add_argument("--output", "-o", help="Output file for results")
    parser.add_argument("--llm-context", action="store_true", help="Generate LLM context")
    
    args = parser.parse_args()
    
    # Initialize search interface
    search_interface = SlackSearchInterface()
    search_interface.initialize(join_channels=not args.no_join, days_back=args.days)
    
    results = {}
    
    # Handle different search modes
    if args.trending:
        results = search_interface.get_trending_topics(args.limit)
    elif args.activity:
        results = search_interface.get_channel_activity()
    elif args.relevant_channels and args.query:
        results = {
            "relevant_channels": search_interface.find_relevant_channels(args.query, args.limit)
        }
    elif args.keywords:
        results = search_interface.search_by_keywords(args.keywords, args.limit)
    elif args.channels:
        results = search_interface.search_by_channel(args.channels, args.query, args.limit)
    elif args.users:
        results = search_interface.search_by_user(args.users, args.query, args.limit)
    elif args.query:
        results = search_interface.search(args.query, args.limit)
        
        # Generate LLM context if requested
        if args.llm_context:
            messages = [Message(**msg) for msg in results.get("results", [])]
            llm_context = search_interface.generate_llm_context(messages, args.query)
            results["llm_context"] = json.loads(llm_context)
    else:
        print("Please provide a search query or specify a search mode")
        return
    
    # Output results
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")
    else:
        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main() 