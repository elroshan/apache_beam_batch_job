import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from src.main import ApplyTransform


class UnitTest(unittest.TestCase):

    TRANSACTIONS = [
                    '2007-01-01 02:54:25 UTC,wallet1,wallet2,100.5',
                    '2007-01-01 04:22:23 UTC,wallet3,wallet4,20.5',
                    '2009-01-01 15:04:11 UTC,wallet7,wallet8,15.50',
                    '2018-01-01 16:04:11 UTC,wallet9,wallet10,25.00'
                    ]

    EXPECTED_OUTPUT = [('2007-01-01', 121.0)]

    def test_main(self):
        with TestPipeline() as p:
            output = (
                    p
                    | 'make pCollection' >> beam.Create(self.TRANSACTIONS)
                    | 'Transform' >> ApplyTransform()
                    )

            assert_that(output, equal_to(self.EXPECTED_OUTPUT), label='CheckOutput')
