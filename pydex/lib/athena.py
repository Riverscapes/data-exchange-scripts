import time
import boto3


def athena_query(s3_bucket: str, query: str):
    """
    Perform an Athena query and return the result.
    """

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

    # Poll for completion
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)  # Wait before polling again

    if state != 'SUCCEEDED':
        print(f"Athena query failed or was cancelled: {state}")
        return None

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
