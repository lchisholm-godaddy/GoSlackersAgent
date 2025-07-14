
import os
import io
import json
import requests
from typing import List, Dict, Optional
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

SLACK_APP_CHANNEL = os.getenv("SLACK_APP_CHANNEL")
SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN")

def get_now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Base function to send messages to Slack. It's just hitting the endpoint with the token and channel
def post_message_to_slack(text: str, blocks: Optional[List[Dict[str, str]]] = None):
    print(os.getenv("SLACK_APP_TOKEN"))
    return requests.post('https://slack.com/api/chat.postMessage', {
        'token': os.getenv("SLACK_APP_TOKEN"),
        'channel': os.getenv("SLACK_APP_CHANNEL"),
        'text': text,
        'blocks': json.dumps(blocks) if blocks else None
    }).json()	

# Same function as above, but insted of just text, it sends files through slack.
def post_file_to_slack(
  text: str, file_name: str, file_bytes: bytes, file_type: Optional[str] = None, title: Optional[str] = None
):
    return requests.post(
      'https://slack.com/api/files.upload', 
      {
        'token': os.getenv("SLACK_APP_TOKEN"),
        'filename': file_name,
        'channels': os.getenv("SLACK_APP_CHANNEL"),
        'filetype': file_type,
        'initial_comment': text,
        'title': title
      },
      files = { 'file': file_bytes }).json()

# Wrapper function to post Matplotlib plots to Slack
def post_matplotlib_to_slack():
    buf = io.BytesIO()
    plt.savefig(buf, format='png', facecolor="white")
    buf.seek(0)
    post_file_to_slack("", "", buf.getvalue())

# Custom functions using Block Kit to structure the messages sent to Slack
# Process start
def post_start_process_to_slack(process_name: str):
    start_time = get_now_str()
    start_block = [
      {
        "type": "header",
        "text": {
          "type": "plain_text",
          "text": "A new process has just started :rocket:",
        }
      },
      {
        "type": "section",
        "fields": [{
            "type": "mrkdwn",
            "text": f"Process _{process_name}_ started at {start_time}"
            }
        ]
        }
    ]

    post_message_to_slack("New process kicked off!", start_block)

# Process end
def post_end_process_to_slack(process_name: str):
    end_time = get_now_str()
    end_block = [
        {
		"type": "header",
		"text": {
			"type": "plain_text",
			"text": "Process successful :large_green_circle:"
		    }
        },
        {
        "type": "section",
        "fields": [{
            "type": "mrkdwn",
            "text": f"Process: _{process_name}_ finished successfully at {end_time}"
            }
        ]
        }
    ]
    post_message_to_slack("Process ended successfully", end_block)

# Process failed
def post_failed_process_to_slack(process_name: str):
    failed_time = get_now_str()
    failed_block = [
        {
		"type": "header",
		"text": {
			"type": "mrkdwn",
			"text": "Process Failed :rotating_light:"
		    }
        },
        {
        "type": "section",
        "fields": [{
            "type": "mrkdwn",
            "text": f"Process: _{process_name}_ failed at {failed_time}"
            }
        ]
        }
    ]
    post_message_to_slack("Process failed!", failed_block)