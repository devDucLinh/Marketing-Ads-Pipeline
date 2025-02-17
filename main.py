import pandas as pd
import json
import gcsfs
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, TimePartitioning
import functions_framework

# Constants
PROJECT_ID = 'testreplaceflow'
LOCATION = 'asia-southeast1'
CM_COLUMN_MAPPING = {
    'Advertiser' : 'advertiser_name',
    'AdvertiserID' : 'advertiser_id',    
    'Campaign' : 'campaign_name',
    'CampaignID' : 'campaign_id',
    'Placement' : 'ad_group_name',
    'PlacementID' : 'ad_group_id',
    'Ad': 'ad_name',
    'AdID': 'ad_id',    
    'Creative': 'creative_name',
    'CreativeID': 'creative_id',
    'Country': 'country',
    'City': 'city',
    'Date': 'date',
    'Impressions': 'impressions',
    'Clicks': 'clicks',
    'VideoFirstQuartileCompletions': 'video_views',
    'VideoCompletions': 'completed_views',
    'AudioFirstQuartileCompletions': 'audio_listens',
    'AudioCompletions': 'completed_listens',
    'CompanionViews': 'ec_impressions',
    'CompanionClicks': 'ec_clicks',
    'account_api_id': 'account_api_id',
}

DV_COLUMN_MAPPING = {
    'AdvertiserID' : 'advertiser_id',    
    'CampaignID' : 'campaign_id',
    'InsertionOrderID' : 'ad_group_id',
    'LineItemID': 'ad_id',    
    'CreativeID': 'creative_id',
    'CountryName': 'country',
    'City': 'city',
    'Date': 'date',
    'BillableImpressions': 'impressions',
    'Clicks': 'clicks',
    'FirstQuartileViews_Video': 'video_views',
    'CompleteViews_Video': 'completed_views',
    'FirstQuartile_Audio': 'audio_listens',
    'CompleteListens_Audio': 'completed_listens',
    'CompanionViews_Video': 'ec_impressions',
    'CompanionClicks_Video': 'ec_clicks',
    'account_api_id': 'account_api_id',
}


IRONSOURCE_COLUMN_MAPPING = {
    'CampaignID' : 'campaign_id',
    'Time': 'date',
    'BillableImpressions': 'impressions',
    'Clicks': 'clicks',
    'FirstQuartile': 'video_views',
    'Complete': 'completed_views',
    'ECImpressions': 'ec_impressions',
    'ECClicks': 'ec_clicks',
    'account_api_id': 'account_api_id',
}



def delete_old_data(client, project_id, dataset_id, table_name, min_date, max_date, partition_field, account_api_id, source=None):
    """Delete existing data based on the last update date."""
    query = f"""
        DELETE FROM `{project_id}.{dataset_id}.{table_name}`
        WHERE {partition_field} >= '{min_date}' AND {partition_field} <= '{max_date}' AND account_api_id = '{account_api_id}'

    """
    if source:
        query += f" AND data_source = '{source}'"
    client.query(query)


def load_data_to_bq(client, df, project_id, dataset_id, table_name, partition_field, source=None):
    """Load data into BigQuery with partitioning."""
    if source:
        df['data_source'] = source
    partitioning = TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field=partition_field)
    job_config = LoadJobConfig(time_partitioning=partitioning, write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    table_id = f'{project_id}.{dataset_id}.{table_name}'
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()

def update_bigquery_table(project_id, dataset_id, table_name, account_api_id, df, partition_field='Date', source=None):
    """Handle the process to update a BigQuery table including log updates and data loading."""
    max_date = df[partition_field].max()
    min_date = df[partition_field].min()
    client = bigquery.Client()
    partition_field_lower = 'date'
    
    full_table_name = f'{account_api_id}_{table_name}'

    delete_old_data(client, project_id, dataset_id, full_table_name, min_date, max_date, partition_field, account_api_id)
    delete_old_data(client, project_id, 'datawarehouse', table_name, min_date, max_date, partition_field_lower, account_api_id)
    delete_old_data(client, project_id, 'datawarehouse', 'campaign_performance', min_date, max_date, partition_field_lower, account_api_id, source=dataset_id)

    load_data_to_bq(client, df, project_id, dataset_id, full_table_name, partition_field, source)
    if dataset_id.startswith('cm') and full_table_name.endswith('report'):
        df = df.rename(columns=CM_COLUMN_MAPPING)
        df = df[list(CM_COLUMN_MAPPING.values())]
    elif dataset_id.startswith('dv') and full_table_name.endswith('report'):
        df = df.rename(columns=DV_COLUMN_MAPPING)
        df = df[list(DV_COLUMN_MAPPING.values())]
    elif dataset_id.startswith('ironsource') and full_table_name.endswith('report'):
        df = df.rename(columns=IRONSOURCE_COLUMN_MAPPING)
        df = df[list(IRONSOURCE_COLUMN_MAPPING.values())]
    else: 
        return

    agg_cols = ['impressions', 'clicks', 'video_views', 'completed_views', 
                'audio_listens', 'completed_listens', 'ec_impressions', 'ec_clicks']

    agg_dict = {}
    for col in agg_cols:
        if col in df.columns:
            agg_dict[col] = 'sum'

    campaign_data = df.groupby(['date', 'campaign_id', 'account_api_id']).agg(agg_dict)
    campaign_data['ctr'] = round((campaign_data['clicks'] / campaign_data['impressions'])*100, 2)
    campaign_data['vcr'] = round((campaign_data['completed_views'] / campaign_data['impressions'])*100, 2)
    campaign_data.reset_index(inplace=True)

    load_data_to_bq(client, df, project_id, 'datawarehouse', table_name, partition_field_lower, source=dataset_id)
    load_data_to_bq(client, campaign_data, project_id, 'datawarehouse', 'campaign_performance', partition_field_lower, source=dataset_id)

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    """
    Processes a Google Cloud Storage event.
    """
    event = cloud_event.data
    file_name = event['name']
    file_path = f'gs://{event["bucket"]}/{file_name}'
    parts = file_name.split('/')
    dataset_id = parts[0]
    table_name = parts[-1].split('.')[0]
    account_api_id = parts[1]
    try:        
        # Process JSON files
        if file_name.endswith('.json'):
            fs = gcsfs.GCSFileSystem(project=PROJECT_ID)
            with fs.open(file_path) as file:
                data = json.load(file)

            if file_name.startswith('cm'):
                col = list(data.keys())[2] 
            elif file_name.startswith('dv'):
                col = list(data.keys())[0]
            else:
                col = None  

            if col is not None:
                df = pd.json_normalize(data, record_path=col)
                            
                for column in df:
                    df[column] = df[column].apply(lambda x: json.dumps(x).encode('utf-8') if isinstance(x, (list, dict, bool)) else x)

        # Process CSV files
        elif file_name.endswith('.csv'):
            if file_name.startswith('cm'):
                df = pd.read_csv(file_path, skiprows=10 , low_memory=False)
                df = df[:-1]                 
            elif file_name.startswith('dv'):
                df = pd.read_csv(file_path , low_memory=False)
                df = df[df['Line Item ID'].notnull()]
            else:
                df = pd.read_csv(file_path , low_memory=False)  

        else:
            df = None  

        # Clean up column names and save to Google BigQuery
        if df is not None:
            for column in df.columns:
                if "ID" in column:
                    df[column] = df[column].astype('string')
                    df[column] = df[column].str.split('.').str[0]
                if "Date" == column or "Time" == column: 
                    df[column] = pd.to_datetime(df[column], errors='coerce')


            for col in df.select_dtypes(include=['float']).columns:
                if all(df[col].dropna().apply(float.is_integer)):
                    df[col] = df[col].astype('Int64')

            df.columns = [c.replace(' ', '').replace('.', '_').replace('(', '_').replace(')', '').replace('-', '') for c in df.columns]

            df['account_api_id'] = account_api_id  
            if file_name.endswith('.json'):
                #tạo cột Date là ngày hiện tại
                df['Date'] = pd.Timestamp.now().normalize() 
                update_bigquery_table(PROJECT_ID, dataset_id, table_name, account_api_id, df, 'Date')
            if dataset_id.startswith('cm') or dataset_id.startswith('dv'):
                update_bigquery_table(PROJECT_ID, dataset_id, table_name, account_api_id, df, 'Date')
            if dataset_id.startswith('ironsource'):
                update_bigquery_table(PROJECT_ID, dataset_id, table_name, account_api_id, df, 'Time')

    except Exception as e:
        print(f"An error occurred: {e}")
