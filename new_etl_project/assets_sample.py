from dagster import asset, AutoMaterializePolicy
from new_etl_project.resources.read_write import read_json, write_csv
import pandas as pd
from datetime import datetime
import logging

@asset(group_name='etl_jobs',compute_kind='dataframe')
def get_locations(context) -> pd.DataFrame:
	location = "datafiles/locations.json"
	df = read_json(location)
	return df

@asset(group_name='etl_jobs',compute_kind='dataframe', auto_materialize_policy=AutoMaterializePolicy.eager(), deps=[get_locations])
def get_regionCodes(context) -> pd.DataFrame:
	location = "datafiles/region_codes.json"
	df = read_json(location)
	return df

@asset(group_name='etl_jobs', compute_kind='dataframe')
def add_region_code_and_timestamp(context, get_locations: pd.DataFrame, get_regionCodes: pd.DataFrame) -> pd.DataFrame:
    # Join dataframes on the "Region" column
    merged_df = pd.merge(get_locations, get_regionCodes, how='left', on='Region')
    
    # Add timestamp column with current timestamp
    merged_df['timestamp'] = datetime.now()
    
    return merged_df

@asset(code_version="1", group_name='etl_jobs')
def write_to_csv(context, add_region_code_and_timestamp: pd.DataFrame):
    output_path = "datafiles/final_locations.csv"
    write_csv(add_region_code_and_timestamp, output_path)

@asset(code_version="2", group_name='etl_jobs')
def write_to_csv(context, add_region_code_and_timestamp: pd.DataFrame):
    output_path = "datafiles/test/final_locations.csv"
    write_csv(add_region_code_and_timestamp, output_path)
	
