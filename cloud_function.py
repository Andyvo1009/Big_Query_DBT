from google.cloud import bigquery
import json
import logging
from datetime import datetime



def webhook(request):
    """HTTP Cloud Function to receive lead created or updated events and store them in BigQuery."""
    logging.error("Function start")

    request_json = request.get_json()
    row = request_json
    row['event']=row['event'].replace(".","_")
    # row["timestamp"] = normalize_ts(row.get("timestamp"))
    client = bigquery.Client()
    row['data']=json.dumps(row.get('data',{}))
    post_row=row.copy()
    target_table=row.get('event').split('_')[0]
    table_id = f"odoo-crm-479010.bronze_layer.raw_{target_table}"
    
    errors = client.insert_rows_json(table_id, [post_row])
    #Logging for debugging purposes
    logging.error(f"ROW: {post_row}")
    
    if errors:
        logging.error(f"BQ ERRORS: {errors}")
        return {"status": "error", "errors": errors}, 500
    
    return {"status": "success"}, 200
