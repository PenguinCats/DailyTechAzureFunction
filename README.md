# arXiv RSS Durable Function

This Azure Function has been converted to use Durable Functions to handle long-running operations that may exceed the 230-second timeout limit.

## How it works

The function is now composed of several components:

1. **HTTP Starter** (`http_trigger_arvix_rss`): Receives the initial HTTP request and starts the orchestration
2. **Orchestrator** (`arxiv_orchestrator`): Manages the workflow and coordinates the activity functions
3. **Activity Functions**: Individual tasks that can be executed independently:
   - `fetch_arxiv_rss_activity`: Fetches RSS content from arXiv
   - `store_raw_rss_activity`: Stores raw RSS content in blob storage
   - `parse_and_store_articles_activity`: Parses RSS and stores individual articles
   - `store_metadata_activity`: Stores processing metadata

## Usage

### Starting the process

Send a POST request to the HTTP trigger endpoint:

```bash
curl -X POST "https://your-function-app.azurewebsites.net/api/http_trigger_arvix_rss" \
  -H "Content-Type: application/json" \
  -d '{
    "category": "cs",
    "ProcessDate": "2024-01-01"
  }'
```

The response will include URLs to check the status:

```json
{
  "id": "unique-instance-id",
  "statusQueryGetUri": "https://your-function-app.azurewebsites.net/runtime/webhooks/durabletask/instances/unique-instance-id",
  "sendEventPostUri": "https://your-function-app.azurewebsites.net/runtime/webhooks/durabletask/instances/unique-instance-id/raiseEvent/{eventName}",
  "terminatePostUri": "https://your-function-app.azurewebsites.net/runtime/webhooks/durabletask/instances/unique-instance-id/terminate",
  "purgeHistoryDeleteUri": "https://your-function-app.azurewebsites.net/runtime/webhooks/durabletask/instances/unique-instance-id"
}
```

### Checking status

You can check the status using the custom status endpoint:

```bash
curl "https://your-function-app.azurewebsites.net/api/status/{instanceId}"
```

Or use the built-in status endpoint:

```bash
curl "https://your-function-app.azurewebsites.net/runtime/webhooks/durabletask/instances/{instanceId}"
```

## Runtime Status Values

- `Running`: The orchestration is currently executing
- `Completed`: The orchestration completed successfully
- `Failed`: The orchestration failed with an error
- `Terminated`: The orchestration was terminated manually

## Benefits of Durable Functions

1. **No timeout limits**: Can run for hours or days if needed
2. **Automatic checkpointing**: Progress is saved, and the function can resume from where it left off
3. **Retry logic**: Built-in retry capabilities for failed activities
4. **Monitoring**: Better visibility into long-running processes
5. **Scalability**: Each activity can scale independently

## Configuration

Make sure your `local.settings.json` (for local development) or Application Settings (for Azure) include:

```json
{
  "AZURE_STORAGE_ACCOUNT_URL": "https://yourstorageaccount.blob.core.windows.net"
}
```

## Dependencies

The function requires the following packages (see `requirements.txt`):
- `azure-functions`
- `azure-functions-durable`
- `azure-storage-blob`
- `azure-identity`
- `feedparser`
- `requests`
