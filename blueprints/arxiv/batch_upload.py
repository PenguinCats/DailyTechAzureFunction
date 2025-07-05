"""
ArXiv-specific batch upload utilities
Contains domain-specific logic for uploading arXiv articles in batches
"""
import asyncio
import json
import logging
from datetime import datetime
from azure.storage.blob.aio import BlobServiceClient as AsyncBlobServiceClient
from shared.storage_utils import get_async_blob_service_client, upload_blob_async


async def batch_upload_articles_async(articles_data: list, process_date: str, category: str, max_concurrency: int = 20) -> list:
    """
    Upload multiple articles concurrently to blob storage
    """
    container_name = "arxiv-data"
    
    async def upload_single_article(article_data: tuple, blob_service_client: AsyncBlobServiceClient) -> dict:
        """Upload a single article and return result"""
        identifier = "unknown"
        try:
            metadata, identifier = article_data
            blob_name = f"{category}/ProcessDate={process_date}/articles/{identifier}.json"
            json_content = json.dumps(metadata, indent=2, ensure_ascii=False)
            
            blob_url = await upload_blob_async(blob_service_client, container_name, blob_name, json_content)
            logging.debug(f"Article {identifier} uploaded successfully")
            
            return {
                "identifier": identifier,
                "url": blob_url,
                "status": "success"
            }
        except Exception as e:
            logging.error(f"Error uploading article {identifier}: {str(e)}")
            return {
                "identifier": identifier,
                "url": "",
                "status": "error",
                "error": str(e)
            }
    
    try:
        # Get async blob service client
        blob_service_client = await get_async_blob_service_client()
        
        # Create semaphore to limit concurrent uploads
        semaphore = asyncio.Semaphore(max_concurrency)
        
        async def upload_with_semaphore(article_data):
            async with semaphore:
                return await upload_single_article(article_data, blob_service_client)
        
        # Create tasks for all uploads
        tasks = [upload_with_semaphore(article_data) for article_data in articles_data]
        
        # Execute all uploads concurrently
        start_time = datetime.now()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        end_time = datetime.now()
        
        # Close the blob service client
        await blob_service_client.close()
        
        # Process results (handle any exceptions)
        processed_results = []
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Upload task failed with exception: {result}")
                processed_results.append({
                    "identifier": "unknown",
                    "url": "",
                    "status": "error",
                    "error": str(result)
                })
            else:
                processed_results.append(result)
        
        # Filter successful uploads
        successful_uploads = [result for result in processed_results if result["status"] == "success"]
        failed_uploads = [result for result in processed_results if result["status"] == "error"]
        
        upload_duration = (end_time - start_time).total_seconds()
        
        if failed_uploads:
            logging.warning(f"{len(failed_uploads)} articles failed to upload")
        
        logging.info(f"Batch upload completed in {upload_duration:.2f}s: {len(successful_uploads)} successful, {len(failed_uploads)} failed")
        
        return successful_uploads
        
    except Exception as e:
        logging.error(f"Error in batch upload: {str(e)}")
        return []


def run_batch_upload_sync(articles_data: list, process_date: str, category: str, max_concurrency: int = 20) -> list:
    """
    Synchronous wrapper for async batch upload (for use in Azure Functions)
    """
    try:
        # Create new event loop for this thread if none exists
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                raise RuntimeError("Event loop is closed")
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        return loop.run_until_complete(
            batch_upload_articles_async(articles_data, process_date, category, max_concurrency)
        )
    except Exception as e:
        logging.error(f"Error in sync batch upload wrapper: {str(e)}")
        return []
