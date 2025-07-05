"""
Shared utilities for Azure Storage operations
Contains only general storage functions, not domain-specific logic
"""
import os
import logging
from azure.storage.blob import BlobServiceClient
from azure.storage.blob.aio import BlobServiceClient as AsyncBlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.identity.aio import DefaultAzureCredential as AsyncDefaultAzureCredential


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
