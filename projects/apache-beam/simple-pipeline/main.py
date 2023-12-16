import apache_beam as beam
from apache_beam.transforms.window import Sessions, TimestampedValue

class TimestampIndexingDoFn(beam.DoFn):
    def process(self, element):
        yield TimestampedValue((element['userId'], element['click']), element['timestamp'])

with beam.Pipeline() as pipeline:
    events = pipeline | beam.Create(
        [
            {"userId": "Andy", "click": 1, "timestamp": 1603112520},  # Event time: 13:02
            {"userId": "Sam", "click": 1, "timestamp": 1603113240},  # Event time: 13:14
            {"userId": "Andy", "click": 1, "timestamp": 1603115820},  # Event time: 13:57
            {"userId": "Andy", "click": 1, "timestamp": 1603113600},  # Event time: 13:20
        ]
    ) 
    # Create a PCollection, this is done by applying a PTransform to the pipeline
    # events | beam.Map(print)

    timestamped_events = events | beam.ParDo(TimestampIndexingDoFn())
    # For the use of windowing we need to index the PCollection based on event time
    # timestamped_events | beam.Map(print)

    windowed_events = timestamped_events | beam.WindowInto(Sessions(gap_size=30*60))
    # Window the events based on session, which is defined as any series of data with no gap within it larger than the `gap_size`
    # windowed_events | beam.Map(print)

    sum_clicks = windowed_events | beam.CombinePerKey(sum)
    sum_clicks | beam.Map(print)
    