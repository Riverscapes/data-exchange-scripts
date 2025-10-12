""" Utility functions to get data from AWS Athena and return it in useful formats
a version/copy of these functions can be found in multiple Riverscapes repositories
* data-exchange-scripts (this one)
* rs-reports-gen 
* athena 
* cybercastor_scripts

Consider porting any improvements to these other repositories. 
"""
import time
import re
import boto3

from rsxml import Logger


def fix_s3_uri(argstr: str) -> str:
    """the parser is messing up s3 paths. this should fix them
    launch.json value (a valid s3 string): "s3://riverscapes-athena/athena_query_results/d40eac38-0d04-4249-8d55-ad34901fee82.csv" 
    agrstr (input to this function) 's3:\\\\riverscapes-athena\\\\athena_query_results\\\\d40eac38-0d04-4249-8d55-ad34901fee82.csv'
    Returns: a valid s3 string 
    """
    # Replace all backslashes with slashes
    uri = argstr.replace("\\", "/")
    # Ensure exactly two slashes after 's3:'
    uri = re.sub(r'^s3:/+', 's3://', uri)
    return uri


def get_s3_file(s3path: str, localpath: str):
    """Download a file from S3 to local path, fixing S3 URI if needed.
    """
    s3_uri = fix_s3_uri(s3path)
    download_file_from_s3(s3_uri, localpath)


def download_file_from_s3(s3_uri: str, local_path: str) -> None:
    """
    Download a file from S3 to a local path.

    Args:
        s3_uri (str): S3 URI of the file, e.g. 's3://bucket/key'.
        local_path (str): Local filesystem path to save the downloaded file.

    Raises:
        ValueError: If s3_uri is not a valid S3 URI.

    Example:
        download_file_from_s3('s3://riverscapes-athena/adhoc/yct_sample4.csv', '/tmp/yct_sample4.csv')
    """
    log = Logger('Download File')
    # Validate and parse S3 URI
    if not isinstance(s3_uri, str) or not s3_uri.startswith('s3://'):
        raise ValueError(f"Invalid S3 URI: {s3_uri}. Must start with 's3://'")
    parts = s3_uri[5:].split('/', 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Invalid S3 URI: {s3_uri}. Must be in format 's3://bucket/key'")
    s3_bucket, s3_key = parts

    log.info(f"Downloading {s3_key} from bucket {s3_bucket} to {local_path}")
    s3 = boto3.client('s3')
    response = s3.head_object(Bucket=s3_bucket, Key=s3_key)
    size_bytes = response['ContentLength']
    log.info(f"ContentType: {response['ContentType']}\t File size: {size_bytes} bytes")
    # TODO if very large, add progress bar
    s3.download_file(s3_bucket, s3_key, local_path)
    log.info("Download complete.")


def parse_athena_results(rows):
    """Convert Athena result rows to a list of dicts.

    Args: 
        rows (list): Raw Athena result rows.

    Returns:
        list[dict]: List of dictionaries representing query results.
    """
    if not rows or len(rows) < 2:
        return []
    headers = [col['VarCharValue'] for col in rows[0]['Data']]
    data = []
    for row in rows[1:]:
        values = [col.get('VarCharValue', None) for col in row['Data']]
        data.append(dict(zip(headers, values)))
    return data


def athena_query_get_path(s3_bucket: str, query: str, max_wait: int = 600) -> str | None:
    """
    Run an Athena query and return the S3 output path to the CSV result file.

    Args:
        s3_bucket (str): S3 bucket for Athena output.
        query (str): SQL query string.
        max_wait (int): Maximum wait time in seconds.

    Returns:
        str | None: S3 path to the CSV result file, or None on failure.
    """
    result = _run_athena_query(s3_bucket, query, max_wait)
    return result[0] if result else None


def athena_query_get_rows(s3_bucket: str, query: str, max_wait: int = 600) -> list | None:
    """
    Run an Athena query and return the raw result rows.

    Args:
        s3_bucket (str): S3 bucket for Athena output.
        query (str): SQL query string.
        max_wait (int): Maximum wait time in seconds.

    Returns:
        list | None: Raw Athena result rows, or None on failure.
    TODO: No results is legitimate and should return an empty list. 
    """
    result = _run_athena_query(s3_bucket, query, max_wait)
    if not result:
        return None
    _, query_execution_id = result

    log = Logger("Athena query")
    athena = boto3.client('athena', region_name='us-west-2')
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

    if results and len(results) > 1:
        return results
    return None


def athena_query_get_parsed(s3_bucket: str, query: str, max_wait: int = 600) -> list[dict] | None:
    """
    Run an Athena query and return parsed results as a list of dictionaries.

    Args:
        s3_bucket (str): S3 bucket for Athena output.
        query (str): SQL query string.
        max_wait (int): Maximum wait time in seconds.

    Returns:
        list[dict] | None: Parsed Athena results, or None on failure.
    """
    rows = athena_query_get_rows(s3_bucket, query, max_wait)
    if rows:
        return parse_athena_results(rows)
    return None


def _run_athena_query(
    s3_bucket: str,
    query: str,
    max_wait: int = 600
) -> tuple[str, str] | None:
    """
    Run an Athena query and wait for completion.

    Args:
        s3_bucket (str): S3 bucket for Athena output.
        query (str): SQL query string.
        max_wait (int): Maximum wait time in seconds.

    Returns:
        tuple[str, str] | None: (output_path, query_execution_id) on success, or None on failure.

    This is core function called by `athena_query_get_path` and `athena_query_get_rows`
    """
    log = Logger("Athena query")

    # Debugging output
    query_length = len(query.encode('utf-8'))
    if query_length < 2000:
        log.debug(f'Query:\n{query}')
    else:
        log.debug(f'Query is {query_length} bytes')
        log.debug(f"Query starts with: {query[:1900]}")
        log.debug(f"Query ends with: {repr(query[-100:])}")
    # print("Full query:")
    # print(query_str)
    # with open("athena_query.sql", "w", encoding="utf-8") as f:
    #     f.write(query_str)

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
            output_path = status['QueryExecution']['ResultConfiguration'].get('OutputLocation', '')
            log.error(f"S3 OutputLocation (may be empty): {output_path}")
            return None
        time.sleep(2)

    if state != 'SUCCEEDED':
        reason = status['QueryExecution']['Status'].get('StateChangeReason', '')
        log.error(f"Athena query failed or was cancelled: {state}. Reason: {reason}")
        return None

    log.debug(f'Query completed at: {time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))}')
    output_path = status['QueryExecution']['ResultConfiguration'].get('OutputLocation', '')
    return output_path, query_execution_id


def athena_execute(s3_bucket: str, query: str, max_wait: int = 600) -> bool:
    """Run a DDL or maintenance query (e.g., MSCK REPAIR TABLE) and return True if succeeded."""
    result = _run_athena_query(s3_bucket, query, max_wait)
    return result is not None

# =========================
# LEGACY FUNCTION
# =========================


def athena_query(s3_bucket: str, query: str):
    """
    DEPRECATED - use one of the other, improved queries. 
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
