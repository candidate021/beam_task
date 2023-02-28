import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import json
from transaction_row import TransactionRow
from datetime import datetime


class TransactionSumByDateTransform(beam.PTransform):
    
    def expand(self, pcoll):
        as_row = (pcoll | 'clean_to_row' >> beam.ParDo(CleanToRowFn()))
        
        trans_over_20 = (as_row | 'transactions_over_20' >> beam.Filter(lambda tran: tran.transaction_amount > 20))
        
        trans_after_2010 = (trans_over_20 | 'exclude_transactions_before_2010' >> beam.Filter(lambda tran: tran.timestamp.year >= 2010))
        
        sum_by_date = (trans_after_2010 | 'sum_by_date' >> beam.GroupBy("timestamp") \
                .aggregate_field("transaction_amount", sum, "total_amount"))
        
        return (sum_by_date | 'format_jsonl' >> beam.Map(lambda tran:
                                                    json.dumps({
                                                        "date": str(tran.timestamp),
                                                        "total_amount": tran.total_amount
                                                    })))
    

class CleanToRowFn(beam.DoFn):
    HEADERS = ["timestamp","origin","destination","transaction_amount"]
        
    def process(self, element):
        as_dict = dict(zip(self.HEADERS, element.split(",")))
        #convert timestamp to date object
        as_dict["timestamp"] = datetime.strptime(as_dict["timestamp"].strip(" UTC"), "%Y-%m-%d %H:%M:%S").date()
        #convert transaction amount to numeric
        as_dict["transaction_amount"] = float(as_dict["transaction_amount"])
        yield TransactionRow(**as_dict)