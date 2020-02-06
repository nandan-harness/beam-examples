from __future__ import division
import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# User Code
class CurrencyConverterDoFn(beam.DoFn):
    def process(self, element):
        dollar = element / 70
        yield round(dollar, 2)


class PrintDollarsDoFn(beam.DoFn):
    def process(self, element):
        print(element)


def run(beam_options):

    # Defining Pipeline
    p = beam.Pipeline(options=beam_options)

    rupees = [3500, 5000, 100, 1000]

    # PCollection and PTransform
    dollars = (p
               | "Create Input List" >> beam.Create(rupees)
               | "Convert Rupees to Dollar" >> beam.ParDo(CurrencyConverterDoFn())
               )

    dollars | "Print Converted Amounts" >> beam.ParDo(PrintDollarsDoFn())

    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    """
    '--project=qa-setup ' \
    '--save-intermediate-result=True ' \
    '--job_name=currency-converter-example ' \
    '--staging_location=gs://harness_supervised_logs_qa/staging ' \
    '--temp_location=gs://harness_supervised_logs_qa/temp ' \
    '--runner=DataflowRunner'
    """

    args, pipeline_args = parser.parse_known_args()

    beam_options = PipelineOptions(pipeline_args)
    run(beam_options)