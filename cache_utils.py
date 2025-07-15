#!/usr/bin/env python3
"""
Cache Management Utility for Slack Data
"""

import argparse
import json
import sys
from cache_manager import SlackCacheManager

def show_cache_stats(cache_file: str = "slack_cache.json"):
    """Show cache statistics"""
    cache_manager = SlackCacheManager(cache_file)
    stats = cache_manager.get_cache_stats()
    
    print(f"Cache Statistics ({cache_file}):")
    print(f"  Total Messages: {stats['total_messages']}")
    print(f"  Total Channels: {stats['total_channels']}")
    print(f"  Cache File Size: {stats['cache_file_size']} bytes")
    print(f"  Last Update: {stats['last_update']}")
    
    if stats['messages_per_channel']:
        print(f"\nMessages per Channel:")
        sorted_channels = sorted(stats['messages_per_channel'].items(), key=lambda x: x[1], reverse=True)
        for channel, count in sorted_channels[:10]:  # Show top 10
            print(f"  #{channel}: {count} messages")
        
        if len(sorted_channels) > 10:
            print(f"  ... and {len(sorted_channels) - 10} more channels")

def clear_cache(cache_file: str = "slack_cache.json"):
    """Clear the cache"""
    cache_manager = SlackCacheManager(cache_file)
    cache_manager.clear_cache()
    print(f"Cache cleared: {cache_file}")

def optimize_cache(cache_file: str = "slack_cache.json", days: int = 90):
    """Remove old messages from cache"""
    cache_manager = SlackCacheManager(cache_file)
    cache_manager.remove_old_messages(days)
    cache_manager.save_cache()
    print(f"Cache optimized: removed messages older than {days} days")

def export_cache(cache_file: str = "slack_cache.json", output_file: str = "cache_export.json"):
    """Export cache to a readable format"""
    cache_manager = SlackCacheManager(cache_file)
    
    export_data = {
        "metadata": {
            "exported_at": cache_manager.cache_data.get("last_update"),
            "total_messages": len(cache_manager.cache_data["messages"]),
            "total_channels": len(cache_manager.cache_data["channels"])
        },
        "channels": cache_manager.cache_data["channels"],
        "messages": cache_manager.cache_data["messages"]
    }
    
    with open(output_file, 'w') as f:
        json.dump(export_data, f, indent=2)
    
    print(f"Cache exported to: {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Manage Slack cache")
    parser.add_argument("--cache-file", default="slack_cache.json", help="Cache file path")
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Show cache statistics")
    
    # Clear command
    clear_parser = subparsers.add_parser("clear", help="Clear cache")
    
    # Optimize command
    optimize_parser = subparsers.add_parser("optimize", help="Remove old messages")
    optimize_parser.add_argument("--days", type=int, default=90, help="Remove messages older than N days")
    
    # Export command
    export_parser = subparsers.add_parser("export", help="Export cache to readable format")
    export_parser.add_argument("--output", default="cache_export.json", help="Output file")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    try:
        if args.command == "stats":
            show_cache_stats(args.cache_file)
        elif args.command == "clear":
            clear_cache(args.cache_file)
        elif args.command == "optimize":
            optimize_cache(args.cache_file, args.days)
        elif args.command == "export":
            export_cache(args.cache_file, args.output)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 