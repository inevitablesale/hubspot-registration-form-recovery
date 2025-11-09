# LearnUpon User Fetcher

A Python application that pulls all users from LearnUpon using pagination, classifies them by status (Active, Deactivated, or Pending Invite), and returns the combined list. The application can be run manually or scheduled to run daily.

## Features

- **Pagination Support**: Automatically fetches all users across multiple pages
- **User Classification**: Classifies users as Active, Deactivated, or Pending Invite based on:
  - Custom data field `active_yes_or_no`
  - Sign-in count and last sign-in date
- **Error Handling**: Robust error handling with configurable retry logic
- **Logging**: Comprehensive logging to both file and console
- **Scheduling**: Built-in daily scheduling capability
- **JSON Output**: Results saved in structured JSON format with summary statistics

## Installation

1. **Clone or download the files** to your local machine
2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

### Environment Variables

Create a `.env` file (or set environment variables) with the following:

```bash
# Required
LEARNUPON_USERNAME=your_username_here
LEARNUPON_PASSWORD=your_password_here
LEARNUPON_SUBDOMAIN=vrmuniversity

# Optional
LEARNUPON_MAX_PAGES=1000
LEARNUPON_MAX_CONSECUTIVE_ERRORS=3
LEARNUPON_REQUEST_TIMEOUT=30
LEARNUPON_SCHEDULE_TIME=09:00
LEARNUPON_OUTPUT_DIR=.
LEARNUPON_LOG_LEVEL=INFO
```

### Configuration Options

- `LEARNUPON_USERNAME`: Your LearnUpon API username (required)
- `LEARNUPON_PASSWORD`: Your LearnUpon API password (required)
- `LEARNUPON_SUBDOMAIN`: LearnUpon subdomain (default: vrmuniversity)
- `LEARNUPON_MAX_PAGES`: Maximum pages to fetch (default: 1000)
- `LEARNUPON_MAX_CONSECUTIVE_ERRORS`: Max consecutive errors before stopping (default: 3)
- `LEARNUPON_REQUEST_TIMEOUT`: Request timeout in seconds (default: 30)
- `LEARNUPON_SCHEDULE_TIME`: Time to run daily schedule (default: 09:00)
- `LEARNUPON_OUTPUT_DIR`: Directory to save output files (default: current directory)
- `LEARNUPON_LOG_LEVEL`: Logging level (default: INFO)

## Usage

### Manual Run

Run the fetcher once:

```bash
python learnupon_user_fetcher.py
```

### Scheduled Run

Run with daily scheduling (runs at 9 AM by default):

```bash
python learnupon_user_fetcher.py --schedule
```

### Using the Scheduled Runner

For production environments, you can use the dedicated scheduled runner:

```bash
python run_scheduled.py
```

This is useful for cron jobs or other scheduling systems.

## Output

The application generates:

1. **JSON File**: `learnupon_users_YYYYMMDD_HHMMSS.json` containing:
   - Timestamp of the run
   - Total user count
   - Summary statistics by status
   - Complete user list with processed data

2. **Log File**: `learnupon_fetcher.log` with detailed execution logs

### Sample Output Structure

```json
{
  "timestamp": "2024-01-15T09:00:00.000000",
  "total_users": 150,
  "summary": {
    "Active": 120,
    "Deactivated": 20,
    "Pending Invite": 10,
    "Total": 150
  },
  "users": [
    {
      "email": "user@example.com",
      "first_name": "John",
      "last_name": "Doe",
      "learupon_user_id": "12345",
      "learupon_account_status": "Active",
      "sign_in_count": 15,
      "last_sign_in_at": "2024-01-10T14:30:00Z",
      "created_at": "2024-01-01T10:00:00Z",
      "updated_at": "2024-01-15T08:45:00Z"
    }
  ]
}
```

## User Classification Logic

Users are classified based on the following logic:

1. **Deactivated**: If `CustomData.active_yes_or_no` is "no"
2. **Active**: If `CustomData.active_yes_or_no` is "yes"
3. **Pending Invite**: If `sign_in_count` is 0 AND `last_sign_in_at` is null/empty
4. **Active**: Default status if no specific indicators are present

## Error Handling

The application includes robust error handling:

- **Network Errors**: Automatic retry with configurable limits
- **API Errors**: Detailed error logging with response codes
- **Data Validation**: Handles malformed user data gracefully
- **Consecutive Errors**: Stops after configurable consecutive failures

## Logging

Logs are written to both:
- **Console**: Real-time progress updates
- **File**: `learnupon_fetcher.log` for historical records

Log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL

## Scheduling

### Built-in Scheduler

The application includes a built-in scheduler that runs daily at the configured time:

```bash
python learnupon_user_fetcher.py --schedule
```

### External Scheduling

For production environments, consider using:

- **Cron** (Linux/macOS):
  ```bash
  0 9 * * * /path/to/python /path/to/run_scheduled.py
  ```

- **Task Scheduler** (Windows):
  - Create a task to run `run_scheduled.py` daily at 9 AM

- **Docker**: Use a container with cron or a scheduling service

## Troubleshooting

### Common Issues

1. **Authentication Errors**:
   - Verify username and password
   - Check subdomain configuration
   - Ensure API access is enabled

2. **Network Timeouts**:
   - Increase `LEARNUPON_REQUEST_TIMEOUT`
   - Check network connectivity
   - Verify LearnUpon API status

3. **No Users Found**:
   - Check if pagination is working correctly
   - Verify API endpoint URL
   - Review logs for error messages

### Debug Mode

Enable debug logging:

```bash
export LEARNUPON_LOG_LEVEL=DEBUG
python learnupon_user_fetcher.py
```

## API Compatibility

This application is designed to work with LearnUpon's REST API v1. The exact API endpoints and response formats may vary by LearnUpon instance configuration.

## Security Notes

- Store credentials securely using environment variables
- Never commit `.env` files to version control
- Use appropriate file permissions for log files
- Consider using API keys instead of passwords if available

## License

This project is provided as-is for educational and business purposes.
# hubspot-registration-form-recovery
