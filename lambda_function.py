import awswrangler as wr
import pandas as pd
import urllib.parse
import os

os_input_s3_clean_data = os.environ['s3_clean_data'] # s3 bucket path to store clean data
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name'] # catalog name
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name'] # catalog table name for json
os_input_data_operation = os.environ['data_operation'] # write append value


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:

        # Creating DF from content
        df_raw = wr.s3.read_json('s3://{}/{}'.format(bucket, key))

        # Extract required columns:
        df_step_1 = pd.json_normalize(df_raw['items'])

        # Write to S3
        wr_response = wr.s3.to_parquet(
            df=df_step_1,
            path=os_input_s3_clean_data,
            dataset=True,
            database=os_input_glue_catalog_db_name,
            table=os_input_glue_catalog_table_name,
            mode=os_input_data_operation
        )

        return wr_response
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}'.format(key, bucket))
        raise e
