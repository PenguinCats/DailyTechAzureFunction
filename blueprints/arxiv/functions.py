"""
ArXiv Blueprint - Main functions for arXiv RSS processing

This blueprint contains all arXiv-related functionality:
- HTTP triggers for starting RSS processing
- Orchestrator for managing the workflow
- Activity functions for individual processing steps
- Status checking endpoints
"""
import azure.functions as func
import azure.durable_functions as df
import logging
import json
import feedparser
import requests
from datetime import datetime, timezone
from shared.storage_utils import get_blob_service_client, upload_blob_with_container_creation
from .batch_upload import run_batch_upload_sync

# Create the blueprint
arxiv_bp = func.Blueprint()


# HTTP starter function to initiate the durable function
@arxiv_bp.route(route="http_trigger_arxiv_rss", methods=["POST"])
@arxiv_bp.durable_client_input(client_name="client")
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


# HTTP endpoint to check orchestration status
@arxiv_bp.route(route="arxiv/status/{instanceId}", methods=["GET"])
@arxiv_bp.durable_client_input(client_name="client")
async def get_arxiv_status(req: func.HttpRequest, client) -> func.HttpResponse:
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


# Orchestrator function that coordinates the workflow
@arxiv_bp.orchestration_trigger(context_name="context")
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
@arxiv_bp.activity_trigger(input_name="category")
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
@arxiv_bp.activity_trigger(input_name="input_data")
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
@arxiv_bp.activity_trigger(input_name="input_data")
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
@arxiv_bp.activity_trigger(input_name="input_data")
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
