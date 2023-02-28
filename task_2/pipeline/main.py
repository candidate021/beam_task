import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import logging
from composite_transform import TransactionSumByDateTransform

def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).runner = "DirectRunner"
    
    gcs_filename = "gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv"
    output_filename = "output/results"
    output_ext = ".jsonl.gz"
    
    with beam.Pipeline(options=options) as p:
        read_gcs = (p | 'read_gcs' >> beam.io.ReadFromText(
            file_pattern=gcs_filename,
            skip_header_lines=1
        ))

        sum_trans_by_date = (read_gcs | 'sum_trans_by_date' >> TransactionSumByDateTransform())
        
        write_jsonl = (sum_trans_by_date | 'write_jsonl' >> beam.io.WriteToText(
                                        file_path_prefix=output_filename,
                                        file_name_suffix=output_ext
                                        ))
        

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    run()