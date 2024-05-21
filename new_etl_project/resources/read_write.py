import pandas as pd
import json

def read_json(location):
    """Reads a JSON file from the specified location."""
    with open(location, 'r') as file:
        data = json.load(file)
    df = pd.DataFrame(data)
    df.reset_index(drop=True, inplace=True)
    return df

def write_csv(df, location):
    """Writes a DataFrame to the specified location with the given file name."""
    df.to_csv(location, index = False)
