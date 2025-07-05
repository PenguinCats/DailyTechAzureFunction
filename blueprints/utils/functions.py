"""
Utils Blueprint - Utility functions for administration and health checks

This blueprint provides common utility functions:
- Health checks
- Configuration information
- Storage administration
"""
import azure.functions as func
import logging
import json
from datetime import datetime, timezone
import os
from shared.storage_utils import get_blob_service_client

# Create utilities blueprint for common functions
utils_bp = func.Blueprint()

@utils_bp.route(route="utils/health", methods=["GET"])
async def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """
    Health check endpoint
    """
    logging.info('Health check requested.')
    
    try:
        # Check various services
        health_status = {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "version": os.getenv("FUNCTIONS_EXTENSION_VERSION", "unknown"),
            "environment": os.getenv("AZURE_FUNCTIONS_ENVIRONMENT", "unknown"),
            "checks": {}
        }
        
        # Check blob storage connectivity
        try:
            blob_client = get_blob_service_client()
            # Simple test to list containers (should work even if no containers exist)
            list(blob_client.list_containers(max_results=1))
            health_status["checks"]["blob_storage"] = "healthy"
        except Exception as e:
            health_status["checks"]["blob_storage"] = f"unhealthy: {str(e)}"
            health_status["status"] = "degraded"
        
        return func.HttpResponse(
            json.dumps(health_status, indent=2),
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Health check failed: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }),
            status_code=500,
            mimetype="application/json"
        )


@utils_bp.route(route="utils/config", methods=["GET"])
async def get_config_info(req: func.HttpRequest) -> func.HttpResponse:
    """
    Get non-sensitive configuration information
    """
    logging.info('Configuration info requested.')
    
    try:
        # Return non-sensitive configuration info
        config_info = {
            "function_app_name": os.getenv("WEBSITE_SITE_NAME", "unknown"),
            "resource_group": os.getenv("WEBSITE_RESOURCE_GROUP", "unknown"),
            "subscription_id": os.getenv("WEBSITE_OWNER_NAME", "unknown").split("+")[0] if os.getenv("WEBSITE_OWNER_NAME") else "unknown",
            "region": os.getenv("REGION_NAME", "unknown"),
            "runtime_version": os.getenv("FUNCTIONS_EXTENSION_VERSION", "unknown"),
            "python_version": os.getenv("FUNCTIONS_WORKER_RUNTIME_VERSION", "unknown"),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        return func.HttpResponse(
            json.dumps(config_info, indent=2),
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error getting config info: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }),
            status_code=500,
            mimetype="application/json"
        )


@utils_bp.route(route="utils/storage/containers", methods=["GET"])
async def list_storage_containers(req: func.HttpRequest) -> func.HttpResponse:
    """
    List storage containers (for debugging/admin purposes)
    """
    logging.info('Storage containers list requested.')
    
    try:
        blob_client = get_blob_service_client()
        containers = []
        
        for container in blob_client.list_containers():
            containers.append({
                "name": container.name,
                "last_modified": container.last_modified.isoformat() if container.last_modified else None,
                "metadata": container.metadata
            })
        
        return func.HttpResponse(
            json.dumps({
                "containers": containers,
                "count": len(containers),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }, indent=2),
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error listing storage containers: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }),
            status_code=500,
            mimetype="application/json"
        )


@utils_bp.route(route="utils/storage/container/{container_name}/blobs", methods=["GET"])
async def list_container_blobs(req: func.HttpRequest) -> func.HttpResponse:
    """
    List blobs in a specific container
    """
    container_name = req.route_params.get('container_name')
    logging.info(f'Blobs list requested for container: {container_name}')
    
    if not container_name:
        return func.HttpResponse(
            json.dumps({
                "error": "Container name is required"
            }),
            status_code=400,
            mimetype="application/json"
        )
    
    try:
        blob_client = get_blob_service_client()
        container_client = blob_client.get_container_client(container_name)
        
        # Get query parameters for pagination
        max_results = req.params.get('max_results', '100')
        try:
            max_results = int(max_results)
        except ValueError:
            max_results = 100
        
        blobs = []
        for blob in container_client.list_blobs(max_results=max_results):
            blobs.append({
                "name": blob.name,
                "size": blob.size,
                "last_modified": blob.last_modified.isoformat() if blob.last_modified else None,
                "content_type": blob.content_settings.content_type if blob.content_settings else None,
                "url": f"{container_client.url}/{blob.name}"
            })
        
        return func.HttpResponse(
            json.dumps({
                "container": container_name,
                "blobs": blobs,
                "count": len(blobs),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }, indent=2),
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error listing blobs in container {container_name}: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "error": str(e),
                "container": container_name,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }),
            status_code=500,
            mimetype="application/json"
        )
