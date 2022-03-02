import argparse
import json
import logging
import apache_beam as beam
from datetime import datetime
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


class WriteToJsonLines(beam.PTransform):
    def __init__(self, file_name, file_name_suffix):
        self._file_name = file_name
        self.file_name_suffix = file_name_suffix

    def expand(self, input_coll):
        return (
                input_coll
                | 'format json' >> beam.Map(json.dumps)
                | 'write to text' >> WriteToText(self._file_name,
                                                 file_name_suffix=self.file_name_suffix,
                                                 compression_type=CompressionTypes.GZIP))


class SplitDoFn(beam.DoFn):
    def process(self, element):
        timestamp, origin, destination, transaction_amount = element.split(",")
        return [{
            'timestamp': datetime.fromisoformat(timestamp.replace(' UTC', '')),
            'origin': origin,
            'destination': destination,
            'transaction_amount': float(transaction_amount.strip())
        }]


def generate_json_from_tuple(final_tuple):
    timestamp, amount = final_tuple
    return {'timestamp': timestamp, 'amount': amount}


def is_transaction_amount_greater_than_20(record):
    return record['transaction_amount'] > 20


def is_before_year_2010(record):
    return record['timestamp'].date().year < 2010


class ApplyTransform(beam.PTransform):
    def expand(self, input_coll):
        a = (
                input_coll
                | 'Split' >> beam.ParDo(SplitDoFn())
                | 'transaction_amount greater than 20' >> beam.Filter(is_transaction_amount_greater_than_20)
                | 'before year 2010' >> beam.Filter(is_before_year_2010)
                | 'make tuples' >> beam.Map(lambda record: (record['timestamp'].strftime("%Y-%m-%d"), record['transaction_amount']))
                | 'GroupAndSum' >> beam.CombinePerKey(sum)
        )
        return a


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv',
        help='Input file to process.')

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        output = (p
                  | 'Read' >> ReadFromText(known_args.input, skip_header_lines=1)
                  | 'Transform' >> ApplyTransform()
                  )

        output \
        | 'Jsonize' >> beam.Map(generate_json_from_tuple) \
        | 'Write to Json Lines' >> WriteToJsonLines(f'../output/results-{datetime.now().strftime("%Y_%m_%d-%H_%M_%S")}', '.jsonl.gz')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
