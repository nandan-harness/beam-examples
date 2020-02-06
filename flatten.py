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

    list_a = p | "List A " >> beam.Create([1, 2, 3])
    list_b = p | "List B " >> beam.Create([4, 5, 6, 7])
    list_c = p | "List C " >> beam.Create([8, 9])

    combined_list = ((list_a, list_b, list_c)
                     | "Flatten" >> beam.Flatten()
                     )

    combined_list | "Print Combined List" >> beam.ParDo(PrintElementDoFn())


    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    run()
