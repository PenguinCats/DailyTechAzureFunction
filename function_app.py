import azure.functions as func
import logging
from datetime import datetime, timezone

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="http_trigger_arvix_rss")
def http_trigger_arvix_rss(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = req.get_json()
        category = req_body.get('category', 'cs')
    except ValueError:
        category = 'cs'

    timestamp = datetime.now(timezone.utc)

    return func.HttpResponse(f"Hello, {timestamp}. This HTTP triggered function executed successfully.")