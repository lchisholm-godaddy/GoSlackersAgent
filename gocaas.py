import requests
import logging
import os
import json
from dotenv import load_dotenv
# Import WebClient from Python SDK (github.com/slackapi/python-slack-sdk)
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

load_dotenv()

app = App(token=os.environ["SLACK_BOT_TOKEN"])

@app.event("app_mention")
def handle_app_mention(event, say):
    user = event["user"]
    global text
    text = event["text"]
    say(f"Hi <@{user}>, working on an answer for your question {text}...")
    analyze_slack_channel()
    response = call_godaddy_api()
    say(f"Here's an answer for your question {text}: {response}")
    

@app.event("message")
def handle_dm(event, say):
    # Only run in DMs (channel_type == 'im')
    if event.get("channel_type") == "im" and event.get("subtype") is None:
        user = event["user"]
        global text
        text = event["text"]
        say(f"Hi <@{user}>, working on an answer for your question {text}...")
        analyze_slack_channel()
        response = call_godaddy_api()
        say(f"Here's an answer for your question {text}: {response}")


# Global variable to store the formatted Slack response
slack_response = ""

def analyze_slack_channel():
    """Analyze Slack channel messages and threads."""
    global slack_response
    slack_response = ""  # Reset the global variable
    
    client = WebClient(token=os.environ.get("SLACK_BOT_TOKEN"))
    
    try:
        # Call the conversations.list method using the WebClient
        bot_response = client.conversations_list()
        
        # Check if the API call was successful
        if not bot_response.get("ok", False):
            error_msg = f"Error: API call failed - {bot_response.get('error', 'Unknown error')}"
            slack_response += error_msg + "\n"
            print(slack_response)
            return
            
        channels = bot_response.get("channels", [])
        if not channels:
            no_channels_msg = "No channels found"
            slack_response += no_channels_msg + "\n"
            print(slack_response)
            return
            
        channel_id = "C09669L640H"
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
        
        summary_msg = f"Found {len(threaded_messages)} threaded messages and {len(standalone_messages)} standalone messages"
        slack_response += summary_msg + "\n"
        
        # Get replies for each threaded message using conversations.replies
        for i, parent_message in enumerate(threaded_messages, 1):
            thread_ts = parent_message.get("ts")
            
            thread_header = f"\n{'='*60}\nTHREAD #{i} - {thread_ts}:\n{'='*60}"
            slack_response += thread_header + "\n"
            
            # Print parent message
            parent_header = "PARENT MESSAGE:"
            slack_response += parent_header + "\n"
            
            parent_details = f"  User: {parent_message.get('user', 'Unknown')}\n  Timestamp: {parent_message.get('ts', 'Unknown')}\n  Text: {parent_message.get('text', 'No text')}\n  Type: {parent_message.get('type', 'Unknown')}"
            slack_response += parent_details + "\n"
            
            if parent_message.get('subtype'):
                subtype_msg = f"  Subtype: {parent_message.get('subtype')}"
                slack_response += subtype_msg + "\n"
            slack_response += "\n"
            
            try:
                # Get all replies using conversations.replies
                replies_result = client.conversations_replies(channel=channel_id, ts=thread_ts)
                replies = replies_result.get("messages", [])
                
                # Remove the parent message from replies (it's included in the response)
                thread_replies = [msg for msg in replies if msg.get("ts") != thread_ts]
                
                if thread_replies:
                    replies_header = f"REPLIES ({len(thread_replies)}):"
                    slack_response += replies_header + "\n"
                    
                    for j, reply in enumerate(thread_replies, 1):
                        reply_header = f"  Reply #{j}:"
                        slack_response += reply_header + "\n"
                        
                        reply_details = f"    User: {reply.get('user', 'Unknown')}\n    Timestamp: {reply.get('ts', 'Unknown')}\n    Text: {reply.get('text', 'No text')}\n    Type: {reply.get('type', 'Unknown')}"
                        slack_response += reply_details + "\n"
                        
                        if reply.get('subtype'):
                            reply_subtype = f"    Subtype: {reply.get('subtype')}"
                            slack_response += reply_subtype + "\n"
                        slack_response += "\n"
                else:
                    no_replies_msg = "No replies in this thread"
                    slack_response += no_replies_msg + "\n"
                    
            except SlackApiError as e:
                error_msg = f"Error getting replies: {e}"
                slack_response += error_msg + "\n"
            
            thread_footer = f"{'='*60}"
            slack_response += thread_footer + "\n"
        
        # Handle standalone messages
        if standalone_messages:
            standalone_header = f"\nSTANDALONE MESSAGES ({len(standalone_messages)}):\n{'='*60}"
            slack_response += standalone_header + "\n"
            
            for i, msg in enumerate(standalone_messages, 1):
                msg_header = f"Message #{i}:"
                slack_response += msg_header + "\n"
                
                msg_details = f"  User: {msg.get('user', 'Unknown')}\n  Timestamp: {msg.get('ts', 'Unknown')}\n  Text: {msg.get('text', 'No text')}\n  Type: {msg.get('type', 'Unknown')}"
                slack_response += msg_details + "\n"
                
                if msg.get('subtype'):
                    msg_subtype = f"  Subtype: {msg.get('subtype')}"
                    slack_response += msg_subtype + "\n"
                slack_response += "\n"

        # Print the entire formatted response at the end
        print(slack_response)

    except SlackApiError as e:
        error_msg = f"Error: {e}"
        slack_response += error_msg + "\n"
        print(slack_response)

def call_godaddy_api():
    """Make API call to GoDaddy CaaS API."""
    # Define the API endpoint URL
    api_url = "https://caas.api.godaddy.com/v1/prompts"
    
    jwt_token = os.environ.get("JWT_TOKEN")
    
    headers = {
        "Authorization": f"sso-jwt {jwt_token}",
        "accept": "application/json",
        "Content-Type": "application/json"
    }
    
    payload = {
        "prompt": f"Question: {text} Context: {slack_response}",
        "provider": "openai_chat",
        "providerOptions": {
            "model": "gpt-3.5-turbo"
        }
    }
    
    try:
        # Make a POST request to the API
        response = requests.post(api_url, headers=headers, json=payload)

        # Check if the request was successful (status code 200)
        if 200 <= response.status_code < 300:
            # Parse the JSON response into a Python dictionary
            data = response.json()
            
            # Print only the request message and response value
            print("Request Message:")
            print(payload["prompt"])
            print("\n" + "="*60)
            print("Response:")
            print(data.get('data', {}).get('value', 'No response value found'))
            global godaddy_response
            godaddy_response = data.get('data', {}).get('value', 'No response value found')
            return godaddy_response
        else:
            print(f"Error: {response.status_code} - {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the API call: {e}")

# def main():
#     """Main function to run both methods."""
#     print("=== SLACK CHANNEL ANALYSIS ===")
#     analyze_slack_channel()
    
#     print("\n" + "="*60)
#     print("=== GODADDY API CALL ===")
#     print("="*60)
    
#     call_godaddy_api()

if __name__ == "__main__":
    handler = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    handler.start()