import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import logging
import json
from transforms import CleanToRow, SumByDate

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
        
        as_row = (read_gcs | 'clean_to_row' >> CleanToRow())
        
        trans_over_20 = (as_row | 'transactions_over_20' >> beam.Filter(lambda tran: tran.transaction_amount > 20))
        
        trans_after_2010 = (trans_over_20 | 'exclude_transactions_before_2010' >> beam.Filter(lambda tran: tran.timestamp.year >= 2010))
        
        sum_by_date = (trans_after_2010 | 'sum_by_date' >> SumByDate())
        
        format_jsonl = (sum_by_date | 'format_jsonl' >> beam.Map(lambda tran:
                                                    json.dumps({
                                                        "date": str(tran.timestamp),
                                                        "total_amount": tran.total_amount
                                                    })))
        
        write_jsonl = (format_jsonl | 'write_jsonl' >> beam.io.WriteToText(
                                        file_path_prefix=output_filename,
                                        file_name_suffix=output_ext
                                        ))
        

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    run()