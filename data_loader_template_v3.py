import logging
import argparse
import re
from datetime import datetime, timedelta
import requests
import json

import apache_beam as beam
from apache_beam.io import ReadFromBigQuery, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery, storage
from apache_beam.options.value_provider import StaticValueProvider
import pandas as pd


class Dummy(beam.DoFn):
    def process(self, element):
        logging.info(element)
        yield element

class TransformDataType(beam.DoFn):
    def process(self, element):
        from datetime import datetime
        element["eventTime"] =  datetime.strptime(element["eventTime"] , "%Y-%m-%dT%H:%M:%S.%f+0000")
        element["recordTime"] = datetime.strptime(element["recordTime"] , "%Y-%m-%dT%H:%M:%S.%f+0000")

        yield element

def getSchemaNames(elements):    
    names = []    
    for i in elements:
        if "name" in i.keys():
            names.append(i["name"])
        if "fields" in i.keys():
            result = getSchemaNames(i["fields"])
            if len(result) > 0:
                names = names + result    
    return names

class RemoveExtraFields(beam.DoFn):

    def removeElementsFromDict(self, jsn, my_keys):
        elements = {}
        
        for k,v in jsn.items():
            if k in my_keys:
                elements.update({k:v})
                if type(v) == list and len(v) > 0:
                    list_ = []
                    for val in v:
                        sub_element = self.removeElementsFromDict(val, my_keys)
                        list_.append(sub_element)
                    elements.update({k:list_})
                if type(v) == dict:
                    sub_element = self.removeElementsFromDict(v, my_keys)
                    elements.update({k:sub_element})
        
        return elements

    def process(self, element, schema_fields):

        element = self.removeElementsFromDict(element, schema_fields)

        yield element

class CallAPI(beam.DoFn):
    def __init__(self, host, apikey):
        self.host = host
        self.apikey = apikey

    def process(self, range_hours):
        import requests
        last_hour_start = range_hours[0]
        last_hour_end = range_hours[1]

        last_hour_start_str = last_hour_start.strftime("%Y-%m-%dT%H:%M:%S.%f+0000")
        last_hour_end_str    = last_hour_end.strftime("%Y-%m-%dT%H:%M:%S.%f+0000")

        headers = {
        'apiKey': self.apikey.get(),
        'Content-Type': 'application/json',
        }

        json_data = {
            'filters': [
                {
                    'property': 'type',
                    'operator': 'EQ',
                    'values': [
                        'ObjectEvent',
                    ],
                },
                {
                    'property': 'recordTime',
                    'operator': 'GT',
                    'values': [
                        last_hour_start_str,
                    ],
                },
                {
                    'property': 'recordTime',
                    'operator': 'LT',
                    'values': [
                        last_hour_end_str,
                    ],
                },
            ],
            'order': {
                'property': 'eventTime',
                'direction': 'DESC',
            },
        }

        response = requests.post(f'{self.host.get()}/epcis-core/rest/events/searches', headers=headers, json=json_data)
        
        if response.status_code == 200:
            response_jsn = json.loads(response.text)
            jsn = response_jsn["events"]
            logging.info(f'Quantity of events found, from: {last_hour_start_str} to: {last_hour_end_str}: {len(jsn)}')
        else:
            logging.warn(f'Something went wrong getting data. From: {last_hour_start_str} to: {last_hour_end_str}. Status code: {response.status_code}, reponse text: {response.text}')
            jsn = []

        return jsn

def GenerateDates(element):
    import pandas as pd
    from datetime import timezone as tz
    from datetime import datetime, timedelta

    start_date = element[0].get()
    end_date = element[1].get()

    if start_date and end_date:
        date_range = pd.date_range(start_date, end_date, tz=tz.utc).sort_values(ascending=False)

        list_of_date = []

        for date in date_range:
            for h in range(0,24):
                date = date.replace(hour=h)
                list_of_date.append(date)
    else:
        last_hour = datetime.today() - timedelta(hours =1)      
        list_of_date = [last_hour]
    return list_of_date

def run():

    class AppOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_value_provider_argument(
                '--host',
                help='An URL like: https://red-at.vizix.io')
            parser.add_value_provider_argument(
                '--apikey',
                help='A valid apikey to access')
            parser.add_value_provider_argument(
                '--output_table',
                help='BigQuery table location to write the output to. For example, your-project-id:your-dataset.your-table-name')
            parser.add_argument(
                '--schema_path',
                help='A Path where is located the JSON Schema')
            parser.add_argument(
                '--partition_column',
                help='A column to partition (if table is not created yet)')
            parser.add_value_provider_argument(
                '--start_date',
                default=None,
                help='A start date from which get data, format=YYYY-MM-DD')
            parser.add_value_provider_argument(
                '--end_date',
                default=None,
                help='An end date from which get data, format=YYYY-MM-DD')

    def getDates(element, output_table):
        #from datetime import datetime, timedelta
        from google.cloud import bigquery
        client = bigquery.Client()


        last_hour_start = element.replace(minute=0, second=0, microsecond=0)
        last_hour_end   = element.replace(minute=59, second=59, microsecond=999999)

        logging.info(f'Start datetime: {last_hour_start}')
        logging.info(f'End datetime: {last_hour_end}')

        output_table = output_table.get()
        query_delete = "DELETE FROM `%s` where recordTime>='%s' and recordTime<='%s'"%(output_table.replace(":","."), last_hour_start, last_hour_end)
        logging.info(f'Delete query to remove existing data: {query_delete}')

        try:
            query_job = client.query(query_delete)
            query_job.result()  
        except Exception as e:
            logging.warning(f'Exception when deleting: {e}')
            pass

        return (last_hour_start, last_hour_end)


    pipeline_options = PipelineOptions()


    with beam.Pipeline(options = pipeline_options) as p:
        user_options = pipeline_options.view_as(AppOptions)

        additional_bq_parameters = {'timePartitioning': {'type': 'DAY', 'field': user_options.partition_column}}

        # Open JSON file with Schema 
        f = open(user_options.schema_path)
        schema = json.load(f)
        bq_schema = {"fields": schema}

        # MAIN PIPELINE LOGIC

        #lines = p | "Get Events" >> beam.Create(get_data(str(user_options.host), str(user_options.apikey), last_hour_start, last_hour_end))
        lines = p | "Initialize process" >> beam.Create([(user_options.start_date, user_options.end_date)])
        lines = lines | "Generate Dates" >> beam.ParDo(GenerateDates)
        lines = lines | beam.Reshuffle()
        lines = lines | "Get Range Dates" >> beam.Map(getDates, user_options.output_table)
        #lines = lines | "print" >> beam.ParDo(Dummy())
        lines = lines | "Get Events from API" >> beam.ParDo(CallAPI(user_options.host, user_options.apikey))
        #lines = lines | "print" >> beam.ParDo(Dummy())
        lines = lines | "transform fields" >> beam.ParDo(TransformDataType())
        lines = lines | "Remove extra fields" >> beam.ParDo(RemoveExtraFields(), getSchemaNames(bq_schema["fields"])) # ONLY for method ="FILE_LOADS" to avoid errors when loading to BigQuery
        #lines = lines | "print" >> beam.ParDo(Dummy())
        lines = lines | "load to bigquery" >> WriteToBigQuery(
            user_options.output_table,
            schema= bq_schema,
            #create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            additional_bq_parameters=additional_bq_parameters,
            ignore_unknown_columns=True,
            method="FILE_LOADS", #custom_gcs_temp_location=StaticValueProvider(str, user_options.gcs_temp_location) # ignore_unknown_columns=True has no efect when method="FILE_LOADS", an extra step: "Remove extra fields" was included to avoid errors.
            #method="STREAMING_INSERTS"
        )



if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()