import os
import subprocess
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from dotenv import load_dotenv

load_dotenv()

app = App(token=os.environ["SLACK_BOT_TOKEN"])

def run_getchannels_script():
    try:
        result = subprocess.run(
            ["python3", "getchannels.py"],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout or "✅ Script ran successfully with no output."
    except subprocess.CalledProcessError as e:
        return f"❌ Script failed:\n{e.stderr}"

# Respond in public/private channels
@app.event("app_mention")
def handle_app_mention(event, say):
    user = event["user"]
    say(f"Hi <@{user}>, running `getchannels.py`...")
    output = run_getchannels_script()
    say(f"```\n{output[:3000]}\n```")

# Respond in DMs
@app.event("message")
def handle_dm(event, say):
    # Only run in DMs (channel_type == 'im')
    if event.get("channel_type") == "im" and event.get("subtype") is None:
        user = event["user"]
        say(f"Hi <@{user}>, running `getchannels.py`...")
        output = run_getchannels_script()
        say(f"```\n{output[:3000]}\n```")

if __name__ == "__main__":
    handler = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    handler.start()