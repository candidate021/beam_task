import os
import sys
test_dir = os.path.dirname(__file__)
parent_dir = os.path.abspath(os.path.join(test_dir, os.pardir))

pipeline_dir = os.path.join(parent_dir, 'pipeline')
sys.path.append(pipeline_dir)

from composite_transform import (
    TransactionSumByDateTransform, CleanToRowFn, TransactionRow
    )

import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from datetime import date


class TestTransactionSumByDate(unittest.TestCase):
    
    def test_transaction_sum_by_date(self):
        test_data = [
            "2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99",
            "2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95",
            "2017-03-18 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22",
            "2017-03-18 14:10:44 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,1.00030",
            "2017-08-31 17:00:09 UTC,wallet00001e494c12b3083634,wallet00005f83196ec58e4ffe,13700000023.08",
            "2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,129.12"
        ]

        expected_result = [
            '{"date": "2017-03-18", "total_amount": 2102.22}',
            '{"date": "2017-08-31", "total_amount": 13700000023.08}',
            '{"date": "2018-02-27", "total_amount": 129.12}'
        ]
        
        with TestPipeline() as p:
            results = (
                p | 'create_test_data' >> beam.Create(test_data)
                  | 'process_transactions' >> TransactionSumByDateTransform()
            )
            assert_that(results, equal_to(
                expected_result
            ))


class TestTransformFilters(unittest.TestCase):

    def test_filter_transactions_over_20(self):
        test_data = [
            TransactionRow(timestamp=date(2009, 1, 9), origin='wallet00000e719adfeaa64b5a', destination='wallet00001866cb7e0f09a890', transaction_amount=1021101.99),
            TransactionRow(timestamp=date(2017, 1, 1), origin='wallet00000e719adfeaa64b5a', destination='wallet00001e494c12b3083634', transaction_amount=19.95),
            TransactionRow(timestamp=date(2017, 3, 18), origin='wallet00001866cb7e0f09a890', destination='wallet00001e494c12b3083634', transaction_amount=2102.22),
            TransactionRow(timestamp=date(2017, 3, 18), origin='wallet00001866cb7e0f09a890', destination='wallet00000e719adfeaa64b5a', transaction_amount=1.0003)
        ]

        expected_result = [
            TransactionRow(timestamp=date(2009, 1, 9), origin='wallet00000e719adfeaa64b5a', destination='wallet00001866cb7e0f09a890', transaction_amount=1021101.99),
            TransactionRow(timestamp=date(2017, 3, 18), origin='wallet00001866cb7e0f09a890', destination='wallet00001e494c12b3083634', transaction_amount=2102.22)
        ]

        with TestPipeline() as p:
            results = (
                p | 'create_test_data' >> beam.Create(test_data)
                  | 'trans_over_20' >> beam.Filter(lambda tran: tran.transaction_amount > 20)
            )
            assert_that(results, equal_to(
                expected_result
            ))
            

    def test_filter_transactions_after_2010(self):
        test_data = [
            TransactionRow(timestamp=date(2009, 1, 9), origin='wallet00000e719adfeaa64b5a', destination='wallet00001866cb7e0f09a890', transaction_amount=1021101.99),
            TransactionRow(timestamp=date(2017, 1, 1), origin='wallet00000e719adfeaa64b5a', destination='wallet00001e494c12b3083634', transaction_amount=19.95),
            TransactionRow(timestamp=date(2017, 3, 18), origin='wallet00001866cb7e0f09a890', destination='wallet00001e494c12b3083634', transaction_amount=2102.22),
            TransactionRow(timestamp=date(2017, 3, 18), origin='wallet00001866cb7e0f09a890', destination='wallet00000e719adfeaa64b5a', transaction_amount=1.0003)
        ]

        expected_result = [
            TransactionRow(timestamp=date(2017, 1, 1), origin='wallet00000e719adfeaa64b5a', destination='wallet00001e494c12b3083634', transaction_amount=19.95),
            TransactionRow(timestamp=date(2017, 3, 18), origin='wallet00001866cb7e0f09a890', destination='wallet00001e494c12b3083634', transaction_amount=2102.22),
            TransactionRow(timestamp=date(2017, 3, 18), origin='wallet00001866cb7e0f09a890', destination='wallet00000e719adfeaa64b5a', transaction_amount=1.0003)
        ]

        with TestPipeline() as p:
            results = (
                p | 'create_test_data' >> beam.Create(test_data)
                  | 'trans_after_2010' >> beam.Filter(lambda tran: tran.timestamp.year >= 2010)
            )
            assert_that(results, equal_to(
                expected_result
            ))


class TestCleanToRowFn(unittest.TestCase):
    def test_clean_to_row_fn(self):
        test_data = [
            "2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99",
            "2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95",
            "2017-03-18 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22",
            "2017-03-18 14:10:44 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,1.00030",
            "2017-08-31 17:00:09 UTC,wallet00001e494c12b3083634,wallet00005f83196ec58e4ffe,13700000023.08",
            "2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,129.12"
        ]

        expected_result = [
            TransactionRow(timestamp=date(2009, 1, 9), origin='wallet00000e719adfeaa64b5a', destination='wallet00001866cb7e0f09a890', transaction_amount=1021101.99),
            TransactionRow(timestamp=date(2017, 1, 1), origin='wallet00000e719adfeaa64b5a', destination='wallet00001e494c12b3083634', transaction_amount=19.95),
            TransactionRow(timestamp=date(2017, 3, 18), origin='wallet00001866cb7e0f09a890', destination='wallet00001e494c12b3083634', transaction_amount=2102.22),
            TransactionRow(timestamp=date(2017, 3, 18), origin='wallet00001866cb7e0f09a890', destination='wallet00000e719adfeaa64b5a', transaction_amount=1.0003),
            TransactionRow(timestamp=date(2017, 8, 31), origin='wallet00001e494c12b3083634', destination='wallet00005f83196ec58e4ffe', transaction_amount=13700000023.08),
            TransactionRow(timestamp=date(2018, 2, 27), origin='wallet00005f83196ec58e4ffe', destination='wallet00001866cb7e0f09a890', transaction_amount=129.12)
        ]

        with TestPipeline() as p:
            results = (
                p | 'create_test_data' >> beam.Create(test_data)
                  | 'clean_to_row_fn' >> beam.ParDo(CleanToRowFn()).with_output_types(TransactionRow)
            )
            assert_that(results, equal_to(
                expected_result
            ))