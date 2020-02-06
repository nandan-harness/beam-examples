from pprint import pprint
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class PrintElementDoFn(beam.DoFn):
    def process(self, element):
        pprint(element)


def partition_fn(input, parts):
    if input > 5:
        return 1
    else:
        return 0


def run():
    # runner details
    options = PipelineOptions()

    # Defining Pipeline
    p = beam.Pipeline(options=options)

    numbers = p | beam.Create([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    partitions = numbers | beam.Partition(partition_fn, 2)

    (partitions[0]
     | "combine partition 0 to list" >> beam.combiners.ToList()
     | "print partition 0" >> beam.ParDo(PrintElementDoFn())
     )
    # partitions[1] | "combine partition 1 to list" >> beam.combiners.ToList() | "print partition 1" >> beam.ParDo()

    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    run()
