# GoFetch

A comprehensive Slack data extraction and caching system designed to collect messages from all accessible channels and generate structured JSON output for LLM processing.

## Overview

GoFetch is a robust Slack bot that automatically discovers, joins, and extracts messages from all available channels in your workspace. It features intelligent caching, rate limiting protection, and generates clean JSON output suitable for AI processing.

## Key Features

- **Multi-Channel Data Extraction**: Automatically discovers and processes all accessible channels
- **Intelligent Caching**: Persistent cache system that only fetches new messages on subsequent runs
- **Rate Limiting Protection**: Ultra-conservative API usage with exponential backoff
- **Thread Extraction**: Complete thread conversations with replies
- **Message Deduplication**: Removes duplicate messages across channels and threads
- **LLM-Optimized Output**: Generates structured JSON perfect for AI processing
- **Cache Management**: Comprehensive utilities for cache inspection and management
- **Flexible Processing Modes**: Cache-only mode and force-process-all-channels options

## Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/yourusername/GoFetch.git
   cd GoFetch
   ```

2. **Create virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**:
   Create a `.env` file in the project root:
   ```bash
   SLACK_BOT_TOKEN=xoxb-your-bot-token-here
   ```

## Slack Bot Setup

### Required Permissions
Your Slack bot needs the following OAuth scopes:
- `channels:read` - List public channels
- `channels:join` - Join public channels  
- `channels:history` - Read channel message history
- `groups:read` - List private channels
- `groups:history` - Read private channel history
- `users:read` - Get user information

### Bot Installation
1. Go to [Slack API](https://api.slack.com/apps)
2. Create a new app or use existing one
3. Add the required OAuth scopes
4. Install the app to your workspace
5. Copy the Bot User OAuth Token

## Usage

### Basic Data Extraction
```bash
python getchannels.py
```

This will:
- Discover all available channels
- Join public channels automatically
- Extract messages from all accessible channels
- Save data to `slack_data.json`
- Update the cache in `slack_cache.json`

### Environment Variables

**CACHE_ONLY**: Work entirely from cache without API calls
```bash
export CACHE_ONLY=true
python getchannels.py
```

**PROCESS_ALL_CHANNELS**: Attempt to process all channels, including private ones
```bash
export PROCESS_ALL_CHANNELS=true
python getchannels.py
```

### Cache Management

**View cache statistics**:
```bash
python cache_utils.py stats
```

**Clear cache**:
```bash
python cache_utils.py clear
```

**Optimize cache**:
```bash
python cache_utils.py optimize
```

**Export cache to JSON**:
```bash
python cache_utils.py export output.json
```

## Output Format

### slack_data.json Structure
```json
{
  "search_query": "",
  "total_messages": 158,
  "channels_included": [
    "general",
    "development",
    "random"
  ],
  "conversations": [
    {
      "channel": "general",
      "channel_id": "C1234567890",
      "standalone_messages": [
        {
          "text": "Hello everyone!",
          "timestamp": "p1642258200000100"
        }
      ],
      "threads": [
        {
          "messages": [
            {
              "text": "This is a thread parent message",
              "timestamp": "p1642258200000100"
            },
            {
              "text": "This is a thread reply",
              "timestamp": "p1642258201000100"
            }
          ]
        }
      ]
    }
  ]
}
```

### Message Filtering

The system automatically filters messages based on:
- **Age**: Only messages from the last 30 days
- **Bot messages**: Excluded by default
- **Empty messages**: Automatically removed
- **Duplicates**: Removed based on content and timestamp

## Caching System

### Cache Benefits
- **10-20x faster** subsequent runs
- **Reduced API calls** - only fetches new messages
- **Persistent storage** - survives restarts
- **Intelligent updates** - incremental message fetching
- **Graceful fallback** - uses cache when API fails

### Cache File Structure
The cache (`slack_cache.json`) stores:
- Channel metadata and last fetch timestamps
- All message data with thread information
- User information (if enabled)
- Last update timestamps

### Cache Behavior
- **First run**: Full data extraction and caching
- **Subsequent runs**: Only fetch messages newer than cached data
- **API failures**: Automatically falls back to cached data
- **Rate limiting**: Uses cached data when API limits are hit

## Rate Limiting Protection

The system implements ultra-conservative rate limiting:
- **10+ second delays** between channels
- **Exponential backoff** on rate limit errors
- **Batch processing** with extended delays
- **Retry logic** with increasing delays
- **Graceful degradation** to cached data

## Advanced Configuration

### Processing Modes

**Regular Mode** (default):
- Processes channels bot is member of
- Attempts to join public channels
- Falls back to cache on failures

**Cache-Only Mode**:
- No API calls
- Works entirely from cache
- Perfect for testing and development

**Process All Channels Mode**:
- Attempts to process every discovered channel
- May fail on private channels
- Useful for comprehensive data collection

### Customizing Filters

Edit the filters in `getchannels.py`:
```python
filters = {
    "exclude_bots": True,
    "start_date": datetime.now() - timedelta(days=30),
    "keywords": ["urgent", "critical"],  # Uncomment to filter by keywords
    "channels": ["general", "development"],  # Uncomment to filter specific channels
}
```

## Troubleshooting

### Common Issues

**Rate Limited**:
- Use `CACHE_ONLY=true` to work from cache
- Increase delays in the code
- Check your bot's rate limit status

**Missing Channels**:
- Use `PROCESS_ALL_CHANNELS=true` to force processing
- Check bot permissions
- Manually add bot to private channels

**Empty Output**:
- Check date filters (default is 30 days)
- Verify bot has message history permissions
- Check cache with `python cache_utils.py stats`

**API Errors**:
- Verify `SLACK_BOT_TOKEN` is correct
- Check bot permissions in Slack
- Ensure bot is installed in workspace

### Debugging

Run with detailed logging to see:
- Which channels are being processed
- Which channels are being skipped
- API errors and rate limiting
- Cache usage statistics

## File Structure

```
GoFetch/
├── getchannels.py          # Main data extraction script
├── cache_manager.py        # Cache management system
├── cache_utils.py          # Cache utilities and CLI
├── slack_data.json         # Generated output file
├── slack_cache.json        # Persistent cache file
├── requirements.txt        # Python dependencies
└── README.md              # This file
```

## Development

### Running Tests
```bash
python -m pytest tests/
```

### Code Style
```bash
black .
flake8 .
```

### Contributing
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review the logs for error messages
3. Use cache utilities to inspect data
4. Open an issue with detailed information
