"""
Main Azure Function App with Blueprint-based Architecture

This function app uses blueprints to organize different functionalities:
- ArXiv Blueprint: Handles arXiv RSS processing with your original batch upload logic
- News Blueprint: Example of how to add news-related functions
- Utils Blueprint: Provides utility functions like health checks and admin operations

Your original arXiv functionality is preserved exactly as it was, just organized better.
All batch upload logic and concurrency handling remains unchanged.

To add new functionality:
1. Create a new folder in blueprints/ (e.g., blueprints/weather/)
2. Create __init__.py and functions.py in that folder
3. Define your functions in the blueprint
4. Register the blueprint in this file
"""

import azure.functions as func
import logging

# Import blueprints
from blueprints.arxiv.functions import arxiv_bp
from blueprints.news.functions import news_bp
from blueprints.utils.functions import utils_bp
from blueprints.abstractParse.functions import abstract_parse_bp

# Configure logging
logging.basicConfig(level=logging.INFO)

# Create the main function app
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# Register blueprints
app.register_blueprint(arxiv_bp)
app.register_blueprint(news_bp)
app.register_blueprint(utils_bp)
app.register_blueprint(abstract_parse_bp)

# Root endpoint for API information
@app.route(route="", methods=["GET"])
async def api_info(req: func.HttpRequest) -> func.HttpResponse:
    """
    Root endpoint providing API information and available endpoints
    """
    import json
    from datetime import datetime, timezone
    
    api_info = {
        "name": "DailyTech Azure Function App",
        "description": "Multi-function app organized with blueprints",
        "version": "2.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "architecture": "Blueprint-based organization",
        "blueprints": {
            "arxiv": {
                "description": "arXiv RSS processing with batch upload (your original logic preserved)",
                "endpoints": [
                    "POST /api/http_trigger_arxiv_rss - Start arXiv RSS processing",
                    "GET /api/arxiv/status/{instanceId} - Get processing status",
                    "POST /api/arxiv/mock/create - Create mock article for testing"
                ],
                "features": [
                    "Batch article upload with configurable concurrency",
                    "Async processing with proper error handling",
                    "RSS parsing and metadata extraction",
                    "Azure Blob Storage integration",
                    "Mock article creation for testing abstractParse"
                ]
            },
            "news": {
                "description": "News aggregation and processing functions (example)",
                "endpoints": [
                    "GET /api/news/headlines?category={category}&country={country} - Get news headlines",
                    "GET /api/news/search?q={query} - Search news articles"
                ],
                "note": "Example blueprint - integrate with real news APIs"
            },
            "utils": {
                "description": "Utility functions for health checks and administration",
                "endpoints": [
                    "GET /api/utils/health - Health check",
                    "GET /api/utils/config - Configuration info",
                    "GET /api/utils/storage/containers - List storage containers",
                    "GET /api/utils/storage/container/{name}/blobs - List blobs in container"
                ]
            },
            "abstractParse": {
                "description": "Simplifies academic article descriptions using Azure OpenAI",
                "endpoints": [
                    "POST /api/abstract/simplify - Simplify article description into easy-to-understand language"
                ],
                "input_format": {
                    "file_url": "Full URL to blob containing article metadata JSON"
                },
                "features": [
                    "Reads article metadata from Azure Blob Storage",
                    "Uses Azure OpenAI to translate complex academic language",
                    "Returns both original and simplified descriptions"
                ]
            }
        },
        "usage": {
            "note": "All endpoints are protected with function-level authentication",
            "auth_header": "x-functions-key",
            "content_type": "application/json"
        },
        "migration_notes": {
            "preserved": "All original arXiv functionality including batch upload is preserved",
            "improved": "Better code organization with blueprints",
            "scalable": "Easy to add new function groups without conflicts"
        }
    }
    
    return func.HttpResponse(
        json.dumps(api_info, indent=2),
        mimetype="application/json"
    )


# Health check endpoint (also available at /api/utils/health)
@app.route(route="health", methods=["GET"])
async def quick_health(req: func.HttpRequest) -> func.HttpResponse:
    """
    Quick health check endpoint
    """
    import json
    from datetime import datetime, timezone
    
    return func.HttpResponse(
        json.dumps({
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "app": "DailyTech Function App v2.0",
            "note": "Use /api/utils/health for detailed health checks"
        }),
        mimetype="application/json"
    )
