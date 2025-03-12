import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging

# Define the table where data will be written
table_spec = 'solid-pact-453100-d7:ds_analysis.customer_transactions'

# Define the schema of the BigQuery table
table_schema = 'transaction_id:STRING, customer_id:STRING, amount:FLOAT, transaction_date:TIMESTAMP'

# Define the pipeline options
options = PipelineOptions(
    runner='DataflowRunner',  # Change to 'DirectRunner' if testing locally
    project='solid-pact-453100-d7',
    region='europe-west1',  # Replace with the correct region (e.g., us-central1)
    temp_location='gs://stanley-dataflow-bucket/temp',
    staging_location='gs://stanley-dataflow-bucket/staging',
    job_name='transactions-dataflow-job'
)

# Define a function to handle invalid data
def format_data(fields):
    # Check if the row has the expected number of columns
    if len(fields) != 4:
        logging.warning(f"Skipping malformed row: {fields}")
        return None

    try:
        # Attempt to convert the amount to a float
        amount = float(fields[2])  # amount is at index 2
    except ValueError:
        # Log the error and assign a default value (e.g., 0.0)
        logging.warning(f"Invalid amount value: {fields[2]}. Assigning default value 0.0.")
        amount = 0.0

    return {
        'transaction_id': fields[0],  # transaction_id is at index 0
        'customer_id': fields[1],     # customer_id is at index 1
        'amount': amount,
        'transaction_date': fields[3]  # transaction_date is at index 3
    }

# Define the Apache Beam pipeline
with beam.Pipeline(options=options) as p:
    # Read the CSV file from Google Cloud Storage
    transactions = (
        p
        | 'Read CSV' >> beam.io.ReadFromText('gs://stanley-dataflow-bucket/transactions.csv', skip_header_lines=1)
        | 'Parse CSV' >> beam.Map(lambda line: line.split(','))
        | 'Format Data' >> beam.Map(format_data)  # Use the format_data function
        | 'Filter Invalid Records' >> beam.Filter(lambda record: record is not None)  # Filter out None values
        | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            table_spec,
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )
    )

print("Pipeline execution complete.")