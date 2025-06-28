import azure.functions as func
import azure.durable_functions as df
import logging
import json
import os
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient
from azure.storage.blob.aio import BlobServiceClient as AsyncBlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.identity.aio import DefaultAzureCredential as AsyncDefaultAzureCredential
import feedparser
import requests
import asyncio
from concurrent.futures import ThreadPoolExecutor

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# HTTP starter function to initiate the durable function
@app.route(route="http_trigger_arxiv_rss", methods=["POST"])
@app.durable_client_input(client_name="client")
async def http_trigger_arxiv_rss(req: func.HttpRequest, client) -> func.HttpResponse:
    logging.info('HTTP trigger function received a request.')

    try:
        req_body = req.get_json()
        category = req_body.get('category', 'cs')
        process_date = req_body.get('ProcessDate')
        
        # ProcessDate is required
        if not process_date:
            return func.HttpResponse(
                json.dumps({
                    "status": "error",
                    "message": "ProcessDate is required in the request body"
                }),
                status_code=400,
                mimetype="application/json"
            )
        
        logging.info(f'Starting arXiv RSS processing for category: {category}, ProcessDate: {process_date}')
        
        # Start the orchestrator
        instance_id = await client.start_new("arxiv_orchestrator", client_input={
            "category": category,
            "process_date": process_date
        })
        
        logging.info(f"Started orchestration with ID = '{instance_id}'.")
        
        # Return the management URLs for the orchestration
        return client.create_check_status_response(req, instance_id)
        
    except Exception as e:
        logging.error(f"Error starting orchestration: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "status": "error",
                "message": f"Error starting orchestration: {str(e)}"
            }),
            status_code=500,
            mimetype="application/json"
        )


# Orchestrator function that coordinates the workflow
@app.orchestration_trigger(context_name="context")
def arxiv_orchestrator(context: df.DurableOrchestrationContext):
    """
    Orchestrator function that manages the arXiv RSS processing workflow
    """
    input_data = context.get_input()
    category = input_data.get("category") # type: ignore
    process_date = input_data.get("process_date") # type: ignore
    
    try:
        # Step 1: Fetch arXiv RSS content
        rss_content = yield context.call_activity("fetch_arxiv_rss_activity", category)
        
        if not rss_content:
            return {
                "status": "error",
                "message": "Failed to fetch arXiv RSS content"
            }
        
        # Step 2: Store raw RSS content
        raw_storage_result = yield context.call_activity("store_raw_rss_activity", {
            "content": rss_content,
            "process_date": process_date,
            "category": category
        })
        
        # Step 3: Parse and store articles
        article_urls = yield context.call_activity("parse_and_store_articles_activity", {
            "rss_content": rss_content,
            "process_date": process_date,
            "category": category
        })
        
        # Step 4: Store metadata
        metadata_result = yield context.call_activity("store_metadata_activity", {
            "article_count": len(article_urls) if article_urls else 0,
            "process_date": process_date,
            "category": category
        })
        
        return {
            "status": "success",
            "message": f"arXiv RSS content processed successfully for {process_date}",
            "articles_stored": len(article_urls) if article_urls else 0,
            "category": category,
            "process_date": process_date,
            "raw_storage_url": raw_storage_result,
            "metadata_url": metadata_result
        }
        
    except Exception as e:
        context.set_custom_status(f"Error: {str(e)}")
        return {
            "status": "error",
            "message": f"Error in orchestrator: {str(e)}"
        }


# Activity function to fetch arXiv RSS content
@app.activity_trigger(input_name="category")
def fetch_arxiv_rss_activity(category: str) -> str:
    """
    Activity function to fetch raw RSS content from arXiv API
    """
    try:
        result = fetch_arxiv_rss(category)
        return result
    except Exception as e:
        logging.error(f"Error in fetch_arxiv_rss_activity: {str(e)}")
        return ""


# Activity function to store raw RSS content
@app.activity_trigger(input_name="input_data")
def store_raw_rss_activity(input_data: dict) -> str:
    """
    Activity function to store raw RSS content in Azure Blob Storage
    """
    try:
        result = store_raw_rss_content(
            input_data["content"], 
            input_data["process_date"], 
            input_data["category"]
        )
        return result
    except Exception as e:
        logging.error(f"Error in store_raw_rss_activity: {str(e)}")
        return ""


# Activity function to parse and store articles
@app.activity_trigger(input_name="input_data")
def parse_and_store_articles_activity(input_data: dict) -> list:
    """
    Activity function to parse RSS content and store individual articles
    """
    try:
        result = parse_and_store_articles(
            input_data["rss_content"],
            input_data["process_date"],
            input_data["category"]
        )
        return result
    except Exception as e:
        logging.error(f"Error in parse_and_store_articles_activity: {str(e)}")
        return []


# Activity function to store metadata
@app.activity_trigger(input_name="input_data")
def store_metadata_activity(input_data: dict) -> str:
    """
    Activity function to store processing metadata
    """
    try:
        result = store_metadata(
            input_data["article_count"],
            input_data["process_date"],
            input_data["category"]
        )
        return result
    except Exception as e:
        logging.error(f"Error in store_metadata_activity: {str(e)}")
        return ""


def fetch_arxiv_rss(category: str) -> str:
    """
    Fetch raw RSS content from arXiv API
    """
    try:
        # arXiv RSS URL format
        rss_url = f"https://rss.arxiv.org/rss/{category}"
        
        logging.info(f"Fetching RSS from: {rss_url}")
        
        # Fetch the raw RSS content
        response = requests.get(rss_url, timeout=30)
        response.raise_for_status()
        
        raw_rss_content = response.text
        
        logging.info(f"Successfully fetched raw RSS content from arXiv, content length: {len(raw_rss_content)} characters")
        return raw_rss_content
        
    except Exception as e:
        logging.error(f"Error fetching arXiv RSS: {str(e)}")
        return ""


def store_raw_rss_content(content: str, process_date: str, category: str) -> str:
    """
    Store the raw RSS content in Azure Blob Storage
    """
    try:
        # Create blob service client using RBAC
        blob_service_client = get_blob_service_client()
        
        # Container name
        container_name = "arxiv-data"
        
        # Create blob name with category and ProcessDate folder structure
        blob_name = f"{category}/ProcessDate={process_date}/rss_raw.xml"
        
        # Upload raw content to blob storage with container creation
        blob_url = upload_blob_with_container_creation(
            blob_service_client, 
            container_name, 
            blob_name, 
            content
        )
        
        logging.info(f"Raw RSS content stored successfully in blob: {blob_name}")
        
        return blob_url
        
    except Exception as e:
        logging.error(f"Error storing raw RSS content in blob: {str(e)}")
        raise e


def parse_and_store_articles(rss_content: str, process_date: str, category: str) -> list:
    """
    Parse RSS content and store individual articles as separate files using async batch upload
    """
    try:
        # Parse the RSS feed
        feed = feedparser.parse(rss_content)
        
        if feed.bozo:
            logging.warning(f"RSS feed parsing warning: {feed.bozo_exception}")
        
        articles_data = []
        
        # Process each article and prepare data for batch upload
        for entry in feed.entries:
            # Extract identifier from the guid field (arXiv RSS standard)
            identifier = None
            
            # arXiv RSS uses guid with format: oai:arXiv.org:2203.01250v3
            if hasattr(entry, 'guid') and entry.guid:
                guid = str(entry.guid)
                if 'oai:arXiv.org:' in guid:
                    identifier = guid.split('oai:arXiv.org:')[-1]
            
            # Fallback to link if guid not available
            if not identifier and hasattr(entry, 'link') and entry.link:
                link = str(entry.link)
                if 'arxiv.org/abs/' in link:
                    identifier = link.split('arxiv.org/abs/')[-1]
            
            # Final fallback: abandon entry if no identifier found
            if not identifier:
                continue
            
            # Extract DOI from arxiv:DOI element
            doi = None
            if hasattr(entry, 'arxiv_doi') and entry.arxiv_doi:
                doi = entry.arxiv_doi
            elif hasattr(entry, 'doi') and entry.doi:
                doi = entry.doi
            
            # Extract creator from dc:creator (Dublin Core namespace)
            creator = None
            if hasattr(entry, 'author') and entry.author:
                creator = entry.author
            elif hasattr(entry, 'authors') and entry.authors:
                # authors is a list of author objects
                author_names = []
                for author in entry.authors:
                    if hasattr(author, 'name') and author.name:
                        author_names.append(author.name)
                if author_names:
                    creator = ', '.join(author_names)
            
            # Extract description (contains arXiv ID, announce type, and abstract)
            description = entry.get('description', '') or entry.get('summary', '')
            
            # Create simplified article metadata with only requested fields
            article_metadata = {
                "identifier": identifier,
                "title": entry.get('title', ''),
                "link": entry.get('link', ''),
                "description": description,
                "creator": creator,
                "doi": doi
            }
            
            # Add to batch data (tuple of metadata and identifier)
            articles_data.append((article_metadata, identifier))
        
        logging.info(f"Parsed {len(articles_data)} articles, starting concurrent batch upload...")
        
        # Use async batch upload with configurable concurrency
        # Adjust max_concurrency based on your needs: higher = faster but more resource intensive
        successful_uploads = run_batch_upload_sync(articles_data, process_date, category, max_concurrency=20)
        
        logging.info(f"Successfully processed and stored {len(successful_uploads)} articles using async batch upload")
        return successful_uploads
        
    except Exception as e:
        logging.error(f"Error parsing and storing articles: {str(e)}")
        return []


def store_article_metadata(metadata: dict, process_date: str, category: str, identifier: str) -> str:
    """
    Store individual article metadata in Azure Blob Storage
    """
    try:
        # Create blob service client using RBAC
        blob_service_client = get_blob_service_client()
        
        # Container name
        container_name = "arxiv-data"
        
        # Create blob name for individual article
        blob_name = f"{category}/ProcessDate={process_date}/articles/{identifier}.json"
        
        # Convert metadata to JSON string
        json_content = json.dumps(metadata, indent=2, ensure_ascii=False)
        
        # Upload to blob storage with container creation
        blob_url = upload_blob_with_container_creation(
            blob_service_client, 
            container_name, 
            blob_name, 
            json_content
        )
        
        logging.info(f"Article metadata stored successfully in blob: {blob_name}")
        
        return blob_url
        
    except Exception as e:
        logging.error(f"Error storing article metadata in blob: {str(e)}")
        return ""


def store_metadata(article_count: int, process_date: str, category: str) -> str:
    """
    Store processing metadata in Azure Blob Storage
    """
    try:
        # Create blob service client using RBAC
        blob_service_client = get_blob_service_client()
        
        # Container name
        container_name = "arxiv-data"
        
        # Create metadata
        metadata = {
            "category": category,
            "process_date": process_date,
            "total_articles": article_count,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "rss_source": f"https://rss.arxiv.org/rss/{category}"
        }
        
        # Create blob name for metadata
        blob_name = f"{category}/ProcessDate={process_date}/meta.json"
        
        # Convert metadata to JSON string
        json_content = json.dumps(metadata, indent=2, ensure_ascii=False)
        
        # Upload to blob storage with container creation
        blob_url = upload_blob_with_container_creation(
            blob_service_client, 
            container_name, 
            blob_name, 
            json_content
        )
        
        logging.info(f"Metadata stored successfully in blob: {blob_name}")
        
        return blob_url
        
    except Exception as e:
        logging.error(f"Error storing metadata in blob: {str(e)}")
        return ""


def get_blob_service_client() -> BlobServiceClient:
    """
    Get BlobServiceClient - uses connection string for local development (Azurite) 
    and RBAC for cloud deployment
    """
    try:
        # Check if we're running locally with Azurite
        azure_web_jobs_storage = os.environ.get('AzureWebJobsStorage')
        if azure_web_jobs_storage and azure_web_jobs_storage == "UseDevelopmentStorage=true":
            # Local development with Azurite
            connection_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            logging.info("Using Azurite connection string for local development")
            return blob_service_client
        
        # Production/cloud environment - use RBAC
        storage_account_url = os.environ.get('AZURE_STORAGE_ACCOUNT_URL')
        if not storage_account_url:
            raise ValueError("AZURE_STORAGE_ACCOUNT_URL environment variable not found")
        
        # Use DefaultAzureCredential for RBAC authentication
        credential = DefaultAzureCredential()
        
        # Create blob service client with managed identity
        blob_service_client = BlobServiceClient(
            account_url=storage_account_url,
            credential=credential
        )
        
        logging.info("Using Azure RBAC authentication for cloud deployment")
        return blob_service_client
        
    except Exception as e:
        logging.error(f"Error creating blob service client: {str(e)}")
        raise e


# HTTP endpoint to check orchestration status
@app.route(route="status/{instanceId}", methods=["GET"])
@app.durable_client_input(client_name="client")
async def get_status(req: func.HttpRequest, client) -> func.HttpResponse:
    instance_id = req.route_params.get('instanceId')
    
    try:
        status = await client.get_status(instance_id)
        
        return func.HttpResponse(
            json.dumps({
                "instanceId": status.instance_id,
                "runtimeStatus": status.runtime_status.name if status.runtime_status else None,
                "input": status.input_,
                "output": status.output,
                "createdTime": status.created_time.isoformat() if status.created_time else None,
                "lastUpdatedTime": status.last_updated_time.isoformat() if status.last_updated_time else None
            }, default=str),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Error getting status for instance {instance_id}: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "status": "error",
                "message": f"Error getting status: {str(e)}"
            }),
            status_code=500,
            mimetype="application/json"
        )


def upload_blob_with_container_creation(blob_service_client: BlobServiceClient, container_name: str, blob_name: str, content: str) -> str:
    """
    Upload content to blob storage with automatic container creation if needed
    """
    try:
        blob_client = blob_service_client.get_blob_client(
            container=container_name, 
            blob=blob_name
        )
        
        try:
            blob_client.upload_blob(content, overwrite=True)
        except Exception as upload_error:
            # Check if container doesn't exist
            if "The specified container does not exist" in str(upload_error):
                logging.info(f"Container '{container_name}' does not exist. Creating it...")
                container_client = blob_service_client.get_container_client(container_name)
                container_client.create_container()
                logging.info(f"Container '{container_name}' created successfully")
                
                # Retry the upload
                blob_client.upload_blob(content, overwrite=True)
            else:
                raise upload_error
        
        return blob_client.url
        
    except Exception as e:
        logging.error(f"Error uploading blob {blob_name}: {str(e)}")
        raise e


async def get_async_blob_service_client() -> AsyncBlobServiceClient:
    """
    Get AsyncBlobServiceClient for concurrent operations
    """
    try:
        # Check if we're running locally with Azurite
        azure_web_jobs_storage = os.environ.get('AzureWebJobsStorage')
        if azure_web_jobs_storage and azure_web_jobs_storage == "UseDevelopmentStorage=true":
            # Local development with Azurite
            connection_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
            blob_service_client = AsyncBlobServiceClient.from_connection_string(connection_string)
            logging.info("Using Azurite connection string for async operations")
            return blob_service_client
        
        # Production/cloud environment - use RBAC
        storage_account_url = os.environ.get('AZURE_STORAGE_ACCOUNT_URL')
        if not storage_account_url:
            raise ValueError("AZURE_STORAGE_ACCOUNT_URL environment variable not found")
        
        # Use DefaultAzureCredential for RBAC authentication
        credential = AsyncDefaultAzureCredential()
        
        # Create blob service client with managed identity
        blob_service_client = AsyncBlobServiceClient(
            account_url=storage_account_url,
            credential=credential
        )
        
        logging.info("Using Azure RBAC authentication for async operations")
        return blob_service_client
        
    except Exception as e:
        logging.error(f"Error creating async blob service client: {str(e)}")
        raise e


async def upload_blob_async(blob_service_client: AsyncBlobServiceClient, container_name: str, blob_name: str, content: str) -> str:
    """
    Upload content to blob storage asynchronously with automatic container creation if needed
    """
    try:
        blob_client = blob_service_client.get_blob_client(
            container=container_name, 
            blob=blob_name
        )
        
        try:
            await blob_client.upload_blob(content, overwrite=True)
        except Exception as upload_error:
            # Check if container doesn't exist
            if "The specified container does not exist" in str(upload_error):
                logging.info(f"Container '{container_name}' does not exist. Creating it...")
                container_client = blob_service_client.get_container_client(container_name)
                await container_client.create_container()
                logging.info(f"Container '{container_name}' created successfully")
                
                # Retry the upload
                await blob_client.upload_blob(content, overwrite=True)
            else:
                raise upload_error
        
        return blob_client.url
        
    except Exception as e:
        logging.error(f"Error uploading blob {blob_name}: {str(e)}")
        raise e


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