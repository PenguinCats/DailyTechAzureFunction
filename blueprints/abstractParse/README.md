# Abstract Parse Blueprint

This blueprint provides functionality to simplify academic article descriptions using Azure OpenAI, making complex research papers more accessible to general audiences.

## Features

- **HTTP Trigger**: POST endpoint to process article descriptions
- **Azure OpenAI Integration**: Uses GPT models to translate academic jargon into simple language
- **Blob Storage Integration**: Reads article metadata directly from Azure Blob Storage
- **Error Handling**: Comprehensive error handling and logging

## Endpoint

### POST /api/abstract/simplify

Simplifies an article's description using Azure OpenAI.

**Request Body:**
```json
{
  "file_url": "https://storageaccount.blob.core.windows.net/container/path/to/article.json"
}
```

**Response:**
```json
{
  "status": "success",
  "original_description": "Complex academic description...",
  "simplified_description": "Easy-to-understand explanation...",
  "article_metadata": {
    "identifier": "2203.01250v3",
    "title": "Article Title",
    "link": "https://arxiv.org/abs/2203.01250",
    "description": "Original description...",
    "creator": "Author Name",
    "doi": "10.1000/example"
  }
}
```

## Setup

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Azure OpenAI
Add the following to your `local.settings.json`:

```json
{
  "Values": {
    "AZURE_OPENAI_ENDPOINT": "https://your-openai-service.openai.azure.com/",
    "AZURE_OPENAI_KEY": "your-api-key-here",
    "AZURE_OPENAI_DEPLOYMENT": "gpt-4"
  }
}
```

### 3. Article Metadata Format
The function expects article metadata in this format:
```json
{
  "identifier": "unique-article-id",
  "title": "Article Title",
  "link": "https://link-to-article",
  "description": "Academic description/abstract to be simplified",
  "creator": "Author Name(s)",
  "doi": "DOI if available"
}
```

## Usage Examples

### Using curl
```bash
curl -X POST "http://localhost:7071/api/abstract/simplify" \
  -H "Content-Type: application/json" \
  -d '{
    "file_url": "https://your-storage.blob.core.windows.net/arxiv-data/cs/ProcessDate=2024-01-15/2203.01250v3.json"
  }'
```

### Using Python
```python
import requests

response = requests.post(
    'http://localhost:7071/api/abstract/simplify',
    json={
        'file_url': 'https://your-storage.blob.core.windows.net/arxiv-data/cs/ProcessDate=2024-01-15/2203.01250v3.json'
    }
)

result = response.json()
print(f"Simplified: {result['simplified_description']}")
```

## Error Handling

The function returns appropriate HTTP status codes:
- **200**: Success
- **400**: Bad request (missing parameters, no description found)
- **404**: Article metadata not found
- **500**: Internal server error (OpenAI issues, storage errors)

## Integration with ArXiv Blueprint

This blueprint works seamlessly with the existing ArXiv blueprint. After the ArXiv processing stores article metadata, you can use those blob URLs with this endpoint to get simplified descriptions.

Example workflow:
1. ArXiv blueprint processes RSS feed and stores articles
2. Get the blob URL for a specific article
3. Use this blueprint to simplify the article's description
4. Present both original and simplified versions to users
