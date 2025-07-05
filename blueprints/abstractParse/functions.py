"""
Abstract Parse Blueprint - Main functions for article description simplification

This blueprint contains functionality to:
- HTTP trigger for processing article descriptions
- Azure OpenAI integration for text simplification
- Blob storage integration for reading article metadata

Required Environment Variables:
- AZURE_OPENAI_ENDPOINT: Your Azure OpenAI service endpoint
- AZURE_OPENAI_KEY: Your Azure OpenAI service API key  
- AZURE_OPENAI_DEPLOYMENT: Name of your GPT model deployment (default: gpt-4)

Example local.settings.json configuration:
{
  "Values": {
    "AZURE_OPENAI_ENDPOINT": "https://your-openai-service.openai.azure.com/",
    "AZURE_OPENAI_KEY": "your-api-key-here",
    "AZURE_OPENAI_DEPLOYMENT": "gpt-4"
  }
}
"""
import azure.functions as func
import logging
import json
import os
from typing import Optional

try:
    from openai import AzureOpenAI
except ImportError:
    # OpenAI package will be available at runtime after pip install
    AzureOpenAI = None

from shared.storage_utils import get_blob_service_client

# Create the blueprint
abstract_parse_bp = func.Blueprint()


@abstract_parse_bp.route(route="abstract/simplify", methods=["POST"])
async def simplify_article_description(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP trigger function to simplify article descriptions using Azure OpenAI
    
    Expected input:
    {
        "file_url": "https://storageaccount.blob.core.windows.net/container/path/to/article.json"
    }
    
    Returns:
    {
        "status": "success",
        "original_description": "...",
        "simplified_description": "...",
        "article_metadata": {...}
    }
    """
    logging.info('Abstract simplification request received.')

    try:
        # Parse request body
        req_body = req.get_json()
        if not req_body:
            return func.HttpResponse(
                json.dumps({
                    "status": "error",
                    "message": "Request body is required"
                }),
                status_code=400,
                mimetype="application/json"
            )
        
        file_url = req_body.get('file_url')
        if not file_url:
            return func.HttpResponse(
                json.dumps({
                    "status": "error",
                    "message": "file_url is required in the request body"
                }),
                status_code=400,
                mimetype="application/json"
            )
        
        logging.info(f'Processing file URL: {file_url}')
        
        # Read article metadata from blob storage
        article_metadata = read_article_metadata_from_url(file_url)
        if not article_metadata:
            return func.HttpResponse(
                json.dumps({
                    "status": "error",
                    "message": "Failed to read article metadata from the provided URL"
                }),
                status_code=404,
                mimetype="application/json"
            )
        
        # Extract description for simplification
        description = article_metadata.get('description', '')
        if not description:
            return func.HttpResponse(
                json.dumps({
                    "status": "error",
                    "message": "No description found in the article metadata"
                }),
                status_code=400,
                mimetype="application/json"
            )
        
        # Simplify the description using Azure OpenAI
        simplified_description = await simplify_text_with_openai(description)
        if not simplified_description:
            return func.HttpResponse(
                json.dumps({
                    "status": "error",
                    "message": "Failed to simplify the description using Azure OpenAI"
                }),
                status_code=500,
                mimetype="application/json"
            )
        
        # Return success response
        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "simplified_description": simplified_description,
                "article_metadata": article_metadata
            }, ensure_ascii=False),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error in simplify_article_description: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "status": "error",
                "message": f"Internal server error: {str(e)}"
            }),
            status_code=500,
            mimetype="application/json"
        )


def read_article_metadata_from_url(file_url: str) -> Optional[dict]:
    """
    Read article metadata from Azure Blob Storage using the provided URL
    
    Args:
        file_url: Full URL to the blob containing article metadata JSON
        
    Returns:
        dict: Article metadata or None if failed
    """
    try:
        # Parse the blob URL to extract container and blob name
        # Expected format: https://storageaccount.blob.core.windows.net/container/path/to/blob
        url_parts = file_url.replace('http://', '').replace('https://', '').split('/')
        if len(url_parts) < 3:
            logging.error(f"Invalid blob URL format: {file_url}")
            return None
        
        # Extract container name and blob path
        container_name = url_parts[1]
        blob_name = '/'.join(url_parts[2:])
        
        logging.info(f"Reading from container: {container_name}, blob: {blob_name}")
        
        # Get blob service client using RBAC
        blob_service_client = get_blob_service_client()
        
        # Get blob client
        blob_client = blob_service_client.get_blob_client(
            container=container_name, 
            blob=blob_name
        )
        
        # Download and parse the blob content
        blob_data = blob_client.download_blob()
        content = blob_data.readall().decode('utf-8')

        # Parse JSON content
        article_metadata = json.loads(content)
        
        logging.info(f"Successfully read article metadata for: {article_metadata.get('identifier', 'unknown')}")
        return article_metadata
        
    except Exception as e:
        logging.error(f"Error reading article metadata from URL {file_url}: {str(e)}")
        return None


async def simplify_text_with_openai(description: str) -> Optional[str]:
    """
    Use Azure OpenAI to simplify academic text into easy-to-understand language
    
    Args:
        description: The original academic description/abstract
        
    Returns:
        str: Simplified description or None if failed
    """
    try:
        # Get Azure OpenAI configuration from environment variables
        azure_openai_endpoint = os.getenv('AZURE_OPENAI_ENDPOINT')
        azure_openai_key = os.getenv('AZURE_OPENAI_KEY')
        azure_openai_deployment = os.getenv('AZURE_OPENAI_DEPLOYMENT', 'gpt-4')
        
        if not azure_openai_endpoint or not azure_openai_key:
            logging.error("Azure OpenAI configuration missing. Please set AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_KEY environment variables.")
            return None
        
        if AzureOpenAI is None:
            logging.error("OpenAI package not available. Please install openai package.")
            return None
        
        # Initialize Azure OpenAI client
        client = AzureOpenAI(
            azure_endpoint=azure_openai_endpoint,
            api_key=azure_openai_key,
            api_version="2024-02-01"
        )
        
        # Create the prompt for simplification
        system_prompt = """You are a helpful assistant that specializes in making complex academic and technical content accessible to general audiences.
Your task is to take academic abstracts or descriptions and rewrite them in simple, clear language that anyone can understand.

Guidelines:
- Use simple words instead of technical jargon
- Explain complex concepts in everyday terms
- Keep the main ideas and findings intact
- Make it engaging and easy to read
- Aim for a reading level that an undergraduate student would understand
- Keep the explanation concise but comprehensive
- The generated answer only contains content and does not require other irrelevant content such as greetings to the user"""

        user_prompt = f"""Please simplify the following academic description into easy-to-understand language:

{description}

Rewrite this in simple, clear terms that anyone can understand while preserving the key information and findings."""

        # Make API call to Azure OpenAI
        response = client.chat.completions.create(
            model=azure_openai_deployment,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            max_tokens=1000,
            temperature=0.7
        )
        
        # Extract the simplified text
        content = response.choices[0].message.content
        if not content:
            logging.error("Azure OpenAI returned empty content")
            return None
            
        simplified_text = content.strip()
        
        logging.info(f"Successfully simplified text using Azure OpenAI. Original length: {len(description)}, Simplified length: {len(simplified_text)}")
        return simplified_text
        
    except Exception as e:
        logging.error(f"Error simplifying text with Azure OpenAI: {str(e)}")
        return None
