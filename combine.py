from pprint import pprint
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class PrintElementDoFn(beam.DoFn):
    def process(self, element):
        pprint(element)


class AverageFn(beam.CombineFn):
  def create_accumulator(self):
    sum = 0
    count = 0
    accumlator = (sum, count)
    return accumlator

  def add_input(self, sum_count, input):
    (sum, count) = sum_count
    return sum + input, count + 1

  def merge_accumulators(self, accumulators):
    sums, counts = zip(*accumulators)
    return sum(sums), sum(counts)

  def extract_output(self, sum_count):
    (sum, count) = sum_count
    return sum / count if count else float('NaN')


def run():
    # runner details
    options = PipelineOptions()

    # Defining Pipeline
    p = beam.Pipeline(options=options)

    numbers = p | beam.Create([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    avg = numbers | 'combine' >> beam.CombineGlobally(AverageFn())

    avg | "print the average" >> beam.ParDo(PrintElementDoFn())

    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    run()
