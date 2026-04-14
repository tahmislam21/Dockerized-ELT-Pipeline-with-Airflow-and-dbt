import pandas as pd


def fetch_data():

    print("Fetching sales data from provided sheet...")
    try:
        df = pd.read_excel('/opt/airflow/api_request/Sales Data.xlsx')
        print("Sheet extracted successfully")
        return df

    except:
        print(f"An error occured in extracting from sheet")
        raise