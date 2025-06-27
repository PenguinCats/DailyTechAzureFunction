import azure.functions as func
import logging
import json
import os
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
import feedparser
import requests

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="http_trigger_arvix_rss")
def http_trigger_arvix_rss(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

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
        
        logging.info(f'Processing arXiv RSS for category: {category}, ProcessDate: {process_date}')
        
        # Fetch arXiv RSS content
        rss_content = fetch_arxiv_rss(category)
        
        if rss_content:
            # Store raw RSS content in Azure Blob Storage
            store_raw_rss_content(rss_content, process_date, category)
            
            # Parse and store individual articles
            article_urls = parse_and_store_articles(rss_content, process_date, category)
            
            # Store metadata
            store_metadata(len(article_urls), process_date, category)
            
            return func.HttpResponse(
                json.dumps({
                    "status": "success",
                    "message": f"arXiv RSS content stored successfully for {process_date}",
                    "articles_stored": len(article_urls),
                    "category": category,
                    "process_date": process_date
                }),
                status_code=200,
                mimetype="application/json"
            )
        else:
            return func.HttpResponse(
                json.dumps({
                    "status": "error",
                    "message": "Failed to fetch arXiv RSS content"
                }),
                status_code=500,
                mimetype="application/json"
            )
            
    except Exception as e:
        logging.error(f"Error processing request: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "status": "error",
                "message": f"Error processing request: {str(e)}"
            }),
            status_code=500,
            mimetype="application/json"
        )


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
        return None


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
        
        # Upload raw content to blob storage
        blob_client = blob_service_client.get_blob_client(
            container=container_name, 
            blob=blob_name
        )
        
        blob_client.upload_blob(content, overwrite=True)
        
        blob_url = blob_client.url
        logging.info(f"Raw RSS content stored successfully in blob: {blob_name}")
        
        return blob_url
        
    except Exception as e:
        logging.error(f"Error storing raw RSS content in blob: {str(e)}")
        raise e


def parse_and_store_articles(rss_content: str, process_date: str, category: str) -> list:
    """
    Parse RSS content and store individual articles as separate files
    """
    try:
        # Parse the RSS feed
        feed = feedparser.parse(rss_content)
        
        if feed.bozo:
            logging.warning(f"RSS feed parsing warning: {feed.bozo_exception}")
        
        article_urls = []
        
        # Process each article
        for entry in feed.entries:
            # Extract identifier from the guid field (arXiv RSS standard)
            identifier = None
            
            # arXiv RSS uses guid with format: oai:arXiv.org:2203.01250v3
            if hasattr(entry, 'guid') and entry.guid:
                guid = entry.guid
                if 'oai:arXiv.org:' in guid:
                    identifier = guid.split('oai:arXiv.org:')[-1]
            
            # Fallback to link if guid not available
            if not identifier and hasattr(entry, 'link') and entry.link:
                if 'arxiv.org/abs/' in entry.link:
                    identifier = entry.link.split('arxiv.org/abs/')[-1]
            
            # Final fallback: abandon entry if no identifier found
            if not identifier:
                continue
            
            # Extract DOI from arxiv:DOI element (arXiv-specific namespace)
            doi = None
            if hasattr(entry, 'arxiv_doi') and entry.arxiv_doi:
                doi = entry.arxiv_doi
            
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
            
            # Store individual article
            article_url = store_article_metadata(article_metadata, process_date, category, identifier)
            if article_url:
                article_urls.append({
                    "identifier": identifier,
                    "url": article_url
                })
        
        logging.info(f"Successfully processed and stored {len(article_urls)} articles")
        return article_urls
        
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
        
        # Upload to blob storage
        blob_client = blob_service_client.get_blob_client(
            container=container_name, 
            blob=blob_name
        )
        
        blob_client.upload_blob(json_content, overwrite=True)
        
        blob_url = blob_client.url
        logging.info(f"Article metadata stored successfully in blob: {blob_name}")
        
        return blob_url
        
    except Exception as e:
        logging.error(f"Error storing article metadata in blob: {str(e)}")
        return None


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
        
        # Upload to blob storage
        blob_client = blob_service_client.get_blob_client(
            container=container_name, 
            blob=blob_name
        )
        
        blob_client.upload_blob(json_content, overwrite=True)
        
        blob_url = blob_client.url
        logging.info(f"Metadata stored successfully in blob: {blob_name}")
        
        return blob_url
        
    except Exception as e:
        logging.error(f"Error storing metadata in blob: {str(e)}")
        return None


def get_blob_service_client() -> BlobServiceClient:
    """
    Get BlobServiceClient using role-based access control (RBAC)
    """
    try:
        # Get storage account URL from environment variable
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
        
        return blob_service_client
        
    except Exception as e:
        logging.error(f"Error creating blob service client: {str(e)}")
        raise e