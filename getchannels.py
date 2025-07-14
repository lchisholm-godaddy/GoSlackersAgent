import logging
import os
import json
from dotenv import load_dotenv
# Import WebClient from Python SDK (github.com/slackapi/python-slack-sdk)
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Load environment variables from .env file
load_dotenv()

# WebClient instantiates a client that can call API methods
# When using Bolt, you can use either `app.client` or the `client` passed to listeners.
# Use environment variable for security - set SLACK_BOT_TOKEN in your environment
client = WebClient(token=os.environ.get("SLACK_BOT_TOKEN"))
logger = logging.getLogger(__name__)

try:
    # Call the conversations.list method using the WebClient
    response = client.conversations_list()
    
    # Check if the API call was successful
    if not response.get("ok", False):
        print(f"Error: API call failed - {response.get('error', 'Unknown error')}")
        exit(1)
        
    channels = response.get("channels", [])
    if not channels:
        print("No channels found")
        exit(0)
        
    conversation_history = []

    # for channel in channels:
    #     # Print all channel information
    #     channel_id = channel['id']
    #     client.conversations_join(channel=channel_id)
    #     result = client.conversations_history(channel=channel_id)
    #     messages = result.get("messages", [])
    #     conversation_history = [msg.get("text", "") for msg in messages if msg.get("text")]
    #     print(conversation_history)
    #     print(f"Channel Name: #{channel['name']}")
    #     print(f"Channel ID: {channel['id']}")
    #     print(f"Channel Purpose: {channel.get('purpose', {}).get('value', 'No purpose set')}")
    #     print(f"Channel Topic: {channel.get('topic', {}).get('value', 'No topic set')}")
    #     print(f"Members Count: {channel.get('num_members', 'Unknown')}")
    #     print(f"Is Private: {channel.get('is_private', False)}")
    #     print(f"Is Archived: {channel.get('is_archived', False)}")
    #     print("-" * 50)

    channel_id = "C095L7HALLB"
    client.conversations_join(channel=channel_id)
    result = client.conversations_history(channel=channel_id)
    messages = result.get("messages", [])
    
    # Find messages that have threads (parent messages)
    threaded_messages = []
    standalone_messages = []
    
    for message in messages:
        thread_ts = message.get("thread_ts")
        ts = message.get("ts")
        
        if thread_ts and thread_ts == ts:
            # This is a parent message with a thread
            threaded_messages.append(message)
        elif not thread_ts:
            # This is a standalone message
            standalone_messages.append(message)
    
    print(f"Found {len(threaded_messages)} threaded messages and {len(standalone_messages)} standalone messages")
    
    # Get replies for each threaded message using conversations.replies
    for i, parent_message in enumerate(threaded_messages, 1):
        thread_ts = parent_message.get("ts")
        
        print(f"\n{'='*60}")
        print(f"THREAD #{i} - {thread_ts}:")
        print(f"{'='*60}")
        
        # Print parent message
        print(f"PARENT MESSAGE:")
        print(f"  User: {parent_message.get('user', 'Unknown')}")
        print(f"  Timestamp: {parent_message.get('ts', 'Unknown')}")
        print(f"  Text: {parent_message.get('text', 'No text')}")
        print(f"  Type: {parent_message.get('type', 'Unknown')}")
        if parent_message.get('subtype'):
            print(f"  Subtype: {parent_message.get('subtype')}")
        print()
        
        try:
            # Get all replies using conversations.replies
            replies_result = client.conversations_replies(channel=channel_id, ts=thread_ts)
            replies = replies_result.get("messages", [])
            
            # Remove the parent message from replies (it's included in the response)
            thread_replies = [msg for msg in replies if msg.get("ts") != thread_ts]
            
            if thread_replies:
                print(f"REPLIES ({len(thread_replies)}):")
                for j, reply in enumerate(thread_replies, 1):
                    print(f"  Reply #{j}:")
                    print(f"    User: {reply.get('user', 'Unknown')}")
                    print(f"    Timestamp: {reply.get('ts', 'Unknown')}")
                    print(f"    Text: {reply.get('text', 'No text')}")
                    print(f"    Type: {reply.get('type', 'Unknown')}")
                    if reply.get('subtype'):
                        print(f"    Subtype: {reply.get('subtype')}")
                    print()
            else:
                print("No replies in this thread")
                
        except SlackApiError as e:
            print(f"Error getting replies: {e}")
        
        print(f"{'='*60}")
    
    print(f"\nSTANDALONE MESSAGES ({len(standalone_messages)}):")
    print(f"{'='*60}")
    for i, msg in enumerate(standalone_messages, 1):
        print(f"Message #{i}:")
        print(f"  User: {msg.get('user', 'Unknown')}")
        print(f"  Timestamp: {msg.get('ts', 'Unknown')}")
        print(f"  Text: {msg.get('text', 'No text')}")
        print(f"  Type: {msg.get('type', 'Unknown')}")
        if msg.get('subtype'):
            print(f"  Subtype: {msg.get('subtype')}")
        print()

except SlackApiError as e:
    print(f"Error: {e}")