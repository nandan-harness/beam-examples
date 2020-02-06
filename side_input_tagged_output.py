from pprint import pprint
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class PrintElementDoFn(beam.DoFn):
    def process(self, element):
        pprint(element)


class GetMappingDoFn(beam.DoFn):
    def process(self, element, mapping_list):
        not_found = ''
        currency = ''
        if element in mapping_list[0]:
            currency = mapping_list[0][element]
        else:
            not_found = element

        yield currency
        yield beam.pvalue.TaggedOutput("not_found", not_found)


def run():
    # runner details
    options = PipelineOptions()

    # Defining Pipeline
    p = beam.Pipeline(options=options)


    mapping = { 'MEX': 'Peso',
                'IND': 'Rupee',
                'US': 'Dollar'
                }

    mapping = p | "Mapping" >>  beam.Create([mapping])
    country = p | "Countries" >> beam.Create(['MEX', 'CHN', 'IND'])

    mapping_result = country | "Get currency" >> beam.ParDo(GetMappingDoFn(), beam.pvalue.AsList(mapping)).with_outputs("not_found", main="currency")

    mapping_result['currency'] | "Currency" >> beam.ParDo(PrintElementDoFn())
    mapping_result['not_found'] | "not found" >> beam.ParDo(PrintElementDoFn())
    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    run()
