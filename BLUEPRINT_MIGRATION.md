# Blueprint Structure Migration

## Overview
Your Azure Function App has been restructured to use blueprints for better organization and scalability. **All your original arXiv functionality is preserved exactly as it was** - only the organization has changed.

## New Structure

```
├── function_app.py                 # Main app with blueprint registration
├── function_app_original.py        # Your original implementation (backup)
├── shared/
│   ├── __init__.py
│   └── storage_utils.py            # General storage utilities only
└── blueprints/
    ├── __init__.py
    ├── arxiv/                      # ArXiv functionality
    │   ├── __init__.py
    │   ├── functions.py           # Main arXiv functions (HTTP triggers, orchestrator, activities)
    │   └── batch_upload.py        # Your original batch upload logic
    ├── news/                       # Example: News functionality
    │   ├── __init__.py
    │   └── functions.py           # News-related functions
    └── utils/                      # Utility functions
        ├── __init__.py
        └── functions.py           # Health checks, admin functions
```

## What's Preserved

✅ **All your original arXiv logic** - exact same functions, same behavior  
✅ **Batch upload functionality** - `run_batch_upload_sync` and `batch_upload_articles_async`  
✅ **Concurrency handling** - configurable max_concurrency=20  
✅ **Error handling** - all try-catch blocks and logging  
✅ **RSS parsing logic** - identical feedparser usage  
✅ **Storage integration** - same RBAC authentication and blob operations  
✅ **Orchestrator workflow** - same durable function pattern  

## What's Improved

🎯 **Better Organization** - Related functions are grouped in blueprints  
🎯 **Separation of Concerns** - Domain-specific logic separated from general utilities  
🎯 **Scalability** - Easy to add new function groups without conflicts  
🎯 **Maintainability** - Cleaner code structure  

## Your Original Endpoints Still Work

| Original Endpoint | New Endpoint | Status |
|-------------------|--------------|--------|
| `POST /api/http_trigger_arxiv_rss` | `POST /api/http_trigger_arxiv_rss` | ✅ Same |
| `GET /api/status/{instanceId}` | `GET /api/arxiv/status/{instanceId}` | ⚠️ Updated path |

## Adding New Functionality

To add a new function group (e.g., weather data):

1. **Create folder structure:**
   ```
   blueprints/weather/
   ├── __init__.py
   └── functions.py
   ```

2. **Create blueprint in `functions.py`:**
   ```python
   import azure.functions as func
   
   weather_bp = func.Blueprint()
   
   @weather_bp.route(route="weather/current", methods=["GET"])
   async def get_current_weather(req: func.HttpRequest) -> func.HttpResponse:
       # Your weather logic here
       pass
   ```

3. **Register in `function_app.py`:**
   ```python
   from blueprints.weather.functions import weather_bp
   app.register_blueprint(weather_bp)
   ```

## Available Endpoints

### ArXiv (Your Original Functionality)
- `POST /api/http_trigger_arxiv_rss` - Start RSS processing  
- `GET /api/arxiv/status/{instanceId}` - Check processing status  

### Utils (New Administrative Functions)
- `GET /api/utils/health` - Health check  
- `GET /api/utils/config` - Configuration info  
- `GET /api/utils/storage/containers` - List storage containers  

### News (Example Blueprint)
- `GET /api/news/headlines` - Get news headlines  
- `GET /api/news/search` - Search news articles  

### General
- `GET /api/` - API information and endpoint list  
- `GET /api/health` - Quick health check  

## Domain-Specific vs General Code

### ArXiv-Specific (in blueprints/arxiv/)
- `run_batch_upload_sync()` - Your batch upload wrapper
- `batch_upload_articles_async()` - Your concurrent upload logic  
- `parse_and_store_articles()` - RSS parsing and article processing  
- All orchestrator and activity functions  

### General Utilities (in shared/)
- `get_blob_service_client()` - Storage client creation  
- `upload_blob_with_container_creation()` - Basic blob upload  
- Generic storage operations  

## Testing

All your existing tests should work with minimal changes:
- Update any direct imports to use the new blueprint structure
- The function logic itself is unchanged

## Migration Benefits

1. **No Functionality Loss** - Everything works exactly as before  
2. **Better Code Organization** - Related functions grouped together  
3. **Easy Scaling** - Add new blueprints without touching existing code  
4. **Improved Maintainability** - Clear separation of concerns  
5. **Team Collaboration** - Different teams can work on different blueprints  

## Questions?

If you need to add more functionality or have questions about the structure, just let me know!
