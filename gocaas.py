import requests
import logging
import os
import json
import subprocess
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
    print(slack_response)
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
    """Run getchannels.py and store its output in global slack_response."""
    global slack_response
    slack_response = ""  # Reset the global variable
    
    try:
        # Run getchannels.py and capture output
        result = subprocess.run(
            ["python3", "getchannels.py"],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Store the output in global variable
        slack_response = result.stdout
        
        # Print the output for debugging
        print("=== GETCHANNELS.PY OUTPUT ===")
        print(slack_response)
        
    except subprocess.CalledProcessError as e:
        error_msg = f"Error running getchannels.py: {e.stderr}"
        slack_response = error_msg
        print(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        slack_response = error_msg
        print(error_msg)

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