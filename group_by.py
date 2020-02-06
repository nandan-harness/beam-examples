from pprint import pprint
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class PrintElementDoFn(beam.DoFn):
    def process(self, element):
        pprint(element)


def run():
    # runner details
    options = PipelineOptions()

    # Defining Pipeline
    p = beam.Pipeline(options=options)

    transaction_list = [
        ('amy', 10),
        ('james', 20),
        ('amy', 40),
        ('carl', 50),
    ]

    # group by key example
    transactions = (p
                    | 'CreatePhones' >> beam.Create(transaction_list)
                    | 'GroupBy Name' >> beam.GroupByKey()
                    )

    transactions | "Print Grouped Transactions" >> beam.ParDo(PrintElementDoFn())

    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    run()
