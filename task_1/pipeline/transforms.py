import apache_beam as beam
from transaction_row import TransactionRow
from datetime import datetime

__all__ = ["CleanToRow", "SumByDate"]

class CleanToRow(beam.DoFn, beam.PTransform):
    HEADERS = ["timestamp","origin","destination","transaction_amount"]
        
    def process(self, element):
        as_dict = dict(zip(self.HEADERS, element.split(",")))
        #convert timestamp to date object
        as_dict["timestamp"] = datetime.strptime(as_dict["timestamp"].strip(" UTC"), "%Y-%m-%d %H:%M:%S").date()
        #convert transaction amount to numeric
        as_dict["transaction_amount"] = float(as_dict["transaction_amount"])
        yield TransactionRow(**as_dict)
        
    def expand(self, pcoll):
        return (pcoll | 'clean_to_row' >> beam.ParDo(self.process).with_output_types(TransactionRow))


class SumByDate(beam.PTransform):
    def expand(self, pcoll):
        return (pcoll | 'sum_by_date' >> beam.GroupBy("timestamp") \
                .aggregate_field("transaction_amount", sum, "total_amount"))