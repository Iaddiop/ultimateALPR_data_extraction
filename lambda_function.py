import boto3
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# List of allowed table names
ALLOWED_TABLES = ['bodyStyle', 'color', 'makeModelYear', 'country', 'plate']
ATHENA_DATA_BASE = "test_db" # replace with your athena database or get in environment variables
OUTPUT_LOCATION = "s3://query-results-bucket/"  # replace with your athena output bucket or get in environment variables


def lambda_handler(event, context):

    try:
        client = boto3.client('athena')

        # Extract filters and other parameters from the event
        table_name = event.get('table_name', 'your_default_table')
        filters = event.get('filters', {})
        limit = event.get('limit', 100)  # recommendation to limit with 100 rows

        # Validate table name
        if not isinstance(table_name, str) or table_name not in ALLOWED_TABLES:
            logging.error(f"There are no table named {table_name} in the results")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Invalid or unauthorized table name'})
            }

        # Construct the WHERE clause based on filters
        where_clauses = []
        for column, value in filters.items():
            if isinstance(value, str):
                where_clauses.append(f"{column} = '{value}'")
            elif isinstance(value, (int, float)):
                where_clauses.append(f"{column} = {value}")
            else:
                logging.error(f'Unsupported data type for column {column}')
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': f'Unsupported data type for column {column}'})
                }

        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

        # Construct the SQL query
        query = f"SELECT * FROM {table_name} WHERE {where_clause} LIMIT {limit};"

        # Start Athena query execution
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': ATHENA_DATA_BASE},
            ResultConfiguration={'OutputLocation': OUTPUT_LOCATION}
        )

        query_execution_id = response['QueryExecutionId']

        # Wait for the query to complete
        client.get_waiter('query_succeeded').wait(QueryExecutionId=query_execution_id)

        # Get the results
        result = client.get_query_results(QueryExecutionId=query_execution_id)

        # Extract the rows from the result
        rows = result['ResultSet']['Rows']
        headers = [col['VarCharValue'] for col in rows[0]['Data']]
        data = [{headers[i]: col['VarCharValue'] for i, col in enumerate(row['Data'])} for row in rows[1:]]

        return {
            'statusCode': 200,
            'body': json.dumps(data)
        }

    except client.exceptions.InvalidRequestException as e:
        logging.error(f'Invalid request : {e}')
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Invalid request', 'message': str(e)})
        }
    except client.exceptions.ExecutionException as e:
        logging.error(f'Query execution failed : {e}')
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Query execution failed', 'message': str(e)})
        }
    except Exception as e:
        logging.error(f'Internal server error : {e}')
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error', 'message': str(e)})
        }
