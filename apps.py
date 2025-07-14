import os
import subprocess
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from dotenv import load_dotenv

load_dotenv()

app = App(token=os.environ["SLACK_BOT_TOKEN"])

@app.event("app_mention")
def handle_app_mention(event, say):
    user = event["user"]

    say(f"Hey <@{user}>, running `getchannels.py` now...")

    try:
        result = subprocess.run(
            ["python3", "getchannels.py"],
            capture_output=True,
            text=True,
            check=True
        )
        output = result.stdout or "✅ Script ran successfully (no output)."
        say(f"Done!\n```{output[:3000]}```")  # Trim to Slack's message limit
    except subprocess.CalledProcessError as e:
        say(f"❌ Error running `getchannels.py`:\n```{e.stderr[:3000]}```")

if __name__ == "__main__":
    handler = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    handler.start()
