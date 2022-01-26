import apache_beam as beam



# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    with beam.Pipeline() as pipeline:

        parts = pipeline | beam.io.ReadFromText('gs://york-project-bucket/jthomson/2021-12-17-16-39/part-r-*')

        count = parts | 'Count all elements' >> beam.combiners.Count.Globally()
        count | beam.Map(print)
        parts | beam.io.WriteToText('gs://york-project-bucket/jthomson/results/pycharm',
                                    file_name_suffix='.csv', header='author, points')

