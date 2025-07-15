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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gocaas.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

app = App(token=os.environ["SLACK_BOT_TOKEN"])

@app.event("app_mention")
def handle_app_mention(event, say):
    logger.info("=== APP MENTION EVENT RECEIVED ===")
    user = event["user"]
    global text
    text = event["text"]
    logger.info(f"User: {user}")
    logger.info(f"Message: {text}")
    
    # Send initial response
    initial_response = f"Hi <@{user}>, working on an answer for your question {text}..."
    logger.info(f"Sending initial response: {initial_response}")
    say(initial_response)
    
    # Analyze slack channel
    logger.info("Starting analyze_slack_channel()")
    analyze_slack_channel()
    logger.info(f"Slack response length: {len(slack_response)} characters")
    print(slack_response)
    
    # Call GoDaddy API
    logger.info("Starting call_godaddy_api()")
    response = call_godaddy_api()
    logger.info(f"GoDaddy API response length: {len(response) if response else 0} characters")
    
    # Send final response
    final_response = f"Here's an answer for your question {text}: {response}"
    logger.info(f"Sending final response length: {len(final_response)} characters")
    say(final_response)
    logger.info("=== APP MENTION EVENT COMPLETED ===")

@app.event("message")
def handle_dm(event, say):
    logger.info("=== DM MESSAGE EVENT RECEIVED ===")
    # Only run in DMs (channel_type == 'im')
    if event.get("channel_type") == "im" and event.get("subtype") is None:
        user = event["user"]
        global text
        text = event["text"]
        logger.info(f"DM from user: {user}")
        logger.info(f"DM message: {text}")
        
        # Send initial response
        initial_response = f"Hi <@{user}>, working on an answer for your question {text}..."
        logger.info(f"Sending DM initial response: {initial_response}")
        say(initial_response)
        
        # Analyze slack channel
        logger.info("Starting analyze_slack_channel() for DM")
        analyze_slack_channel()
        logger.info(f"Slack response for DM length: {len(slack_response)} characters")
        
        # Call GoDaddy API
        logger.info("Starting call_godaddy_api() for DM")
        response = call_godaddy_api()
        logger.info(f"GoDaddy API response for DM length: {len(response) if response else 0} characters")
        
        # Send final response
        final_response = f"Here's an answer for your question {text}: {response}"
        logger.info(f"Sending DM final response length: {len(final_response)} characters")
        say(final_response)
        logger.info("=== DM MESSAGE EVENT COMPLETED ===")
    else:
        logger.info(f"Ignoring message - channel_type: {event.get('channel_type')}, subtype: {event.get('subtype')}")

# Global variable to store the formatted Slack response
slack_response = ""

def analyze_slack_channel():
    """Run getchannels.py and store its output in global slack_response."""
    logger.info("=== ANALYZE_SLACK_CHANNEL START ===")
    global slack_response
    slack_response = ""  # Reset the global variable
    logger.info("Reset global slack_response variable")
    
    try:
        logger.info("About to run getchannels.py subprocess")
        # Run getchannels.py and capture output
        # result = subprocess.run(
        #     ["python3", "getchannels.py"],
        #     capture_output=True,
        #     text=True,
        #     check=True
        # )
        # logger.info("getchannels.py subprocess completed successfully")
        
        # Store the output in global variable
        # slack_response = result.stdout
        # logger.info(f"Stored subprocess stdout in slack_response: {len(slack_response)} characters")
        
        # # Print the output for debugging
        # logger.info("=== GETCHANNELS.PY OUTPUT START ===")
        # print(slack_response)
        # logger.info("=== GETCHANNELS.PY OUTPUT END ===")
        
        # if result.stderr:
        #     logger.warning(f"getchannels.py stderr: {result.stderr}")
        
        # Since subprocess is commented out, read from file directly
        logger.info("Subprocess is commented out, reading from slack_data.json file")
        if os.path.exists("slack_data.json"):
            with open("slack_data.json", "r") as f:
                slack_response = f.read()
                logger.info(f"Read slack_data.json: {len(slack_response)} characters")
                
            # Print the output for debugging
            logger.info("=== SLACK_DATA.JSON CONTENT START ===")
            print(slack_response[:1000] + "..." if len(slack_response) > 1000 else slack_response)
            logger.info("=== SLACK_DATA.JSON CONTENT END ===")
        else:
            logger.error("slack_data.json file not found")
            slack_response = "No Slack data available - slack_data.json file not found"
        
    except subprocess.CalledProcessError as e:
        error_msg = f"Error running getchannels.py: {e.stderr}"
        logger.error(error_msg)
        slack_response = error_msg
        print(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg)
        slack_response = error_msg
        print(error_msg)
    
    logger.info("=== ANALYZE_SLACK_CHANNEL END ===")

def call_godaddy_api():
    """Make API call to GoDaddy CaaS API."""
    logger.info("=== CALL_GODADDY_API START ===")
    
    # Define the API endpoint URL
    api_url = "https://caas.api.godaddy.com/v1/prompts"
    jwt_token = os.environ.get("JWT_TOKEN")
    
    logger.info(f"API URL: {api_url}")
    logger.info(f"JWT Token present: {bool(jwt_token)}")
    logger.info(f"JWT Token length: {len(jwt_token) if jwt_token else 0}")
    
    headers = {
        "Authorization": f"sso-jwt {jwt_token}",
        "accept": "application/json",
        "Content-Type": "application/json"
    }
    logger.info("Headers prepared for API call")
    
    # Comprehensive system prompt for Slack analysis
    system_prompt = """You are a Slack workspace analyst. You have access to structured conversation data from multiple Slack channels.

DATA STRUCTURE:
The JSON contains:
- "conversations": Array of channels with their messages
- Each conversation has:
  - "channel": Channel name (e.g., "general", "bugs-template")
  - "channel_id": Channel ID (e.g., "C095QUL2Q5Q")
  - "standalone_messages": Direct messages in the channel
  - "threads": Grouped reply conversations
- Each message has:
  - "text": The actual message content
  - "timestamp": Message timestamp for URL construction

RESPONSE REQUIREMENTS:
1. KEEP RESPONSES SHORT AND FOCUSED - Give a comprehensive answer that directly relates to the question asked. Only use information from the provided data.
2. ALWAYS MENTION the specific channel name where you found the information
3. INCLUDE RELEVANT SLACK URLS using this exact format: https://slack.com/archives/{channel_id}/{timestamp}
4. EXTRACT channel_id and timestamp from the JSON data to build proper URLs
5. MAXIMUM 2-3 sentences for the main response

URL CONSTRUCTION:
- Get channel_id from the conversation data
- Get timestamp from the message data
- Format: https://slack.com/archives/{channel_id}/{timestamp}
- Example: https://slack.com/archives/C095QUL2Q5Q/1234567890.123456

ANALYSIS PROCESS:
1. READ the user's question carefully
2. SCAN the provided JSON data for relevant information
3. IDENTIFY the most helpful messages that directly answer the question
4. EXTRACT channel_id and timestamp to build proper URLs
5. PROVIDE a short, focused response with channel attribution

RESPONSE FORMAT:
- Brief, comprehensive answer (2-3 sentences max)
- Channel reference: "Found in #channel-name"
- If no relevant information found: "I couldn't find relevant information in the available Slack conversations"

IMPORTANT: Only use information from the provided JSON data. Build URLs from the channel_id and timestamp in the data."""
    
    # Read the slack data from file if subprocess is commented out
    if not slack_response.strip():
        logger.info("slack_response is empty, reading from slack_data.json file")
        try:
            with open("slack_data.json", "r") as f:
                slack_data = f.read()
                logger.info(f"Read slack_data.json: {len(slack_data)} characters")
        except FileNotFoundError:
            logger.error("slack_data.json file not found")
            slack_data = "No Slack data available"
        except Exception as e:
            logger.error(f"Error reading slack_data.json: {e}")
            slack_data = "Error reading Slack data"
    else:
        slack_data = slack_response
    
    # Build the complete prompt
    complete_prompt = f"""SYSTEM INSTRUCTIONS:
{system_prompt}

USER QUESTION:
{text}

SLACK WORKSPACE DATA:
{slack_data}

Please analyze the above Slack data and answer the user's question."""
    
    logger.info(f"Complete prompt length: {len(complete_prompt)} characters")
    logger.info(f"User question: {text}")
    logger.info(f"Slack data length: {len(slack_data)} characters")
    
    payload = {
        "prompt": complete_prompt,
        "provider": "openai_chat",
        "providerOptions": {
            "model": "gpt-4.1"
        }
    }
    logger.info("Payload prepared for API call")
    logger.info(f"Model: {payload['providerOptions']['model']}")
    
    try:
        logger.info("Making POST request to GoDaddy API...")
        # Make a POST request to the API
        response = requests.post(api_url, headers=headers, json=payload)
        logger.info(f"API response status code: {response.status_code}")

        # Check if the request was successful (status code 200)
        if 200 <= response.status_code < 300:
            logger.info("API call successful")
            # Parse the JSON response into a Python dictionary
            data = response.json()
            logger.info("Response JSON parsed successfully")
            
            # Print only the request message and response value
            logger.info("=== API REQUEST/RESPONSE START ===")
            print("Request Message:")
            print(complete_prompt)
            print("\n" + "="*60)
            print("Response:")
            response_value = data.get('data', {}).get('value', 'No response value found')
            print(response_value)
            logger.info("=== API REQUEST/RESPONSE END ===")
            
            global godaddy_response
            godaddy_response = response_value
            logger.info(f"Stored API response in global variable: {len(godaddy_response)} characters")
            logger.info("=== CALL_GODADDY_API END (SUCCESS) ===")
            return godaddy_response
        else:
            error_msg = f"Error: {response.status_code} - {response.text}"
            logger.error(error_msg)
            print(error_msg)
            logger.info("=== CALL_GODADDY_API END (ERROR) ===")
            return "❌ API call failed."

    except requests.exceptions.RequestException as e:
        error_msg = f"An error occurred during the API call: {e}"
        logger.error(error_msg)
        print(error_msg)
        logger.info("=== CALL_GODADDY_API END (EXCEPTION) ===")
        return "❌ Request error."

# def main():
#     """Main function to run both methods."""
#     print("=== SLACK CHANNEL ANALYSIS ===")
#     analyze_slack_channel()
    
#     print("\n" + "="*60)
#     print("=== GODADDY API CALL ===")
#     print("="*60)
    
#     call_godaddy_api()

if __name__ == "__main__":
    logger.info("=== STARTING GOCAAS BOT ===")
    logger.info("Initializing SocketModeHandler...")
    handler = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    logger.info("Starting SocketModeHandler...")
    handler.start()
    logger.info("SocketModeHandler started successfully")