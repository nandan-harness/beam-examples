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

    emails_list = [
        ('amy', 'amy@example.com'),
        ('carl', 'carl@example.com'),
        ('julia', 'julia@example.com'),
        ('carl', 'carl-info@email.com'),
    ]

    transaction_list = [
        ('amy', 10),
        ('james', 20),
        ('amy', 40),
        ('carl', 50),
    ]

    emails = p | 'CreateEmails' >> beam.Create(emails_list)
    transactions = p | 'CreatePhones' >> beam.Create(transaction_list)

    # co group by key example
    co_group_result = ({'emails': emails, 'transactions': transactions}
                       | beam.CoGroupByKey())

    co_group_result | "Print Co-Group Transactions" >> beam.ParDo(PrintElementDoFn())

    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    run()
