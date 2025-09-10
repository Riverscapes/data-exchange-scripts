import time
import boto3
from  rsxml import Logger

def parse_athena_results(rows):
    """Convert Athena result rows to a list of dicts."""
    if not rows or len(rows) < 2:
        return []
    headers = [col['VarCharValue'] for col in rows[0]['Data']]
    data = []
    for row in rows[1:]:
        values = [col.get('VarCharValue', None) for col in row['Data']]
        data.append(dict(zip(headers, values)))
    return data

def athena_query(s3_bucket: str, query: str, return_output_path: bool = False, max_wait: int = 600) -> list | str | None:
    """
    Perform an Athena query and return the result rows or the S3 output path.

    Args:
        s3_bucket (str): S3 bucket for Athena output.
        query (str): SQL query string.
        return_output_path (bool): If True, return the S3 path to the CSV result file.
                                   If False (default), return the query results as rows.
        max_wait # longest want to wait, in seconds, before giving up 600 seconds = 10 minutes

    Returns:
        list or str or None: Query results (list of rows) or S3 output path (str), or None on failure.
        the list is not a very friendly output - you have to dig into it.
        For a single row with one column named (result of query "select count(*) from raw_rme where ...") it looks like:
            [{'Data': [{'VarCharValue': '_col0'}]}, {'Data': [{'VarCharValue': '21958477'}]}]
        Suggest to use the parse_athena_results to help
    
    Usage: 
    # For small queries
    raw_results = athena_query(s3_bucket, query)
    parsed_results = parse_athena_results(raw_results)
    print(parsed_results)

    # For large queries
    output_path = athena_query(s3_bucket, query, return_output_path=True)
    print(f"Results at: {output_path}")
    """
    log = Logger("Athena query")
    athena = boto3.client('athena', region_name='us-west-2')
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': 'default',
            'Catalog': 'AwsDataCatalog'
        },
        ResultConfiguration={
            'OutputLocation': f's3://{s3_bucket}/athena_query_results'
        }
    )

    query_execution_id = response['QueryExecutionId']

    start_time = time.time()
    log.debug(f"Query started at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")
    # Poll for completion
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        if time.time() - start_time > max_wait:
            log.error(f"Timed out waiting for Athena query to complete. Waited {max_wait} seconds.")
            log.error(f"Check the Athena console for QueryExecutionId {query_execution_id} for more details.")
            log.error(f"Query string (truncated): {query[:500]}{'...' if len(query) > 500 else ''}")
            output_path = status['QueryExecution']['ResultConfiguration'].get('OutputLocation','')
            log.error(f"S3 OutputLocation (may be empty): {output_path}")
            return None
        time.sleep(2)  # Wait before polling again

    if state != 'SUCCEEDED':
        reason = status['QueryExecution']['Status'].get('StateChangeReason', '')
        log.error(f"Athena query failed or was cancelled: {state}. Reason: {reason}")
        return None

    log.debug(f'Query completed at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))}')
    output_path = status['QueryExecution']['ResultConfiguration'].get('OutputLocation','')

    if return_output_path:
        return output_path

    log.debug('Processing results')
    results = []
    next_token = None
    while True:
        if next_token:
            response = athena.get_query_results(QueryExecutionId=query_execution_id, NextToken=next_token)
        else:
            response = athena.get_query_results(QueryExecutionId=query_execution_id)
        results.extend(response['ResultSet']['Rows'])
        next_token = response.get('NextToken')
        if not next_token:
            break

    # Athena returns header row as first row, data as second row
    rows = results
    if rows and len(rows) > 1:
        return results

    return None
