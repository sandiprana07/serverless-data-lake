
import apache_beam as beam
from dateutil import parser
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import logging
import ast


class ParseTrafficData(beam.DoFn):

    def unify_date_format(self, date_str):
        from dateutil import parser
        try:
            # Auto-parse various date formats
            parsed_date = parser.parse(date_str)
            # Format to desired format: yy-mm-dd
            return parsed_date.strftime('%Y-%m-%d')
        except Exception as e:
            print(f"Error parsing '{date_str}': {e}")
            return None
        
    def safe_float(self, value):
        try:
            return float(value.strip()) if value.strip() else 0.0
        except ValueError:
            return 0.0 
        
    def safe_int(self, value):
        try:
            return int(value.strip()) if value.strip() else 0
        except ValueError:
            return 0     
        
    def process(self, element):
        import ast
        element = element.decode('utf-8') if isinstance(element, bytes) else element
        try:
            record = ast.literal_eval(element)
        except Exception as e:
            logging.warning(f"Skipping malformed row: {element} -- {e}")
            return

        try:
            raw_day = record.get('day')
            parsed_day = self.unify_date_format(raw_day)

            raw_interval = record.get('interval')
            parsed_interval = self.safe_int(raw_interval)

            raw_vehicle_count = record.get('flow')
            parsed_vehicle_count = self.safe_float(raw_vehicle_count)

            raw_error = record.get('error')
            parsed_error = self.safe_float(raw_error)

            raw_location = record.get('city')
            parsed_location = str(raw_location.strip()) if raw_location.strip() else str("No City")

            raw_speed = record.get('speed')
            parsed_speed = self.safe_float(raw_speed)
        
            yield {
                'day': parsed_day,
                'interval' : parsed_interval,
                'vehicle_count': parsed_vehicle_count,
                'error' : parsed_error,
                'location': parsed_location,
                'speed' : parsed_speed
            }
        except Exception as e:
            logging.error(f"Error processing record: {record} -- {e}")

def run():
    options = PipelineOptions(
        streaming=True,
        project='certain-mission-456820-a8',
        region='asia-south1',
        job_name='traffic-data-bq-pipeline',
        temp_location='gs://traffic-data-raw-bucket/temp',
        staging_location='gs://traffic-data-raw-bucket/staging',
        save_main_session=True
    )
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic='projects/certain-mission-456820-a8/topics/traffic-data-topic')
            | 'ParseTrafficData' >> beam.ParDo(ParseTrafficData())
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                table='certain-mission-456820-a8:traffic_dataset.traffic_table',
                schema='day:DATE,interval:INTEGER,vehicle_count:DECIMAL,error:DECIMAL,location:STRING,speed:DECIMAL',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run()
