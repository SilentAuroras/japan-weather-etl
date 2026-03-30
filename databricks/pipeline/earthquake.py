import requests
import pandas as pd
from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType

# Define Databricks schema for earthquake data - pulled DDL from quake.parquet
EARTHQUAKE_SCHEMA = StructType([
    StructField("Control.Title", StringType(), True),
    StructField("Control.DateTime", StringType(), True),
    StructField("Control.Status", StringType(), True),
    StructField("Control.EditorialOffice", StringType(), True),
    StructField("Control.PublishingOffice", StringType(), True),
    StructField("Head.Title", StringType(), True),
    StructField("Head.ReportDateTime", StringType(), True),
    StructField("Head.TargetDateTime", StringType(), True),
    StructField("Head.EventID", StringType(), True),
    StructField("Head.InfoType", StringType(), True),
    StructField("Head.Serial", StringType(), True),
    StructField("Head.InfoKind", StringType(), True),
    StructField("Head.InfoKindVersion", StringType(), True),
    StructField("Head.Headline.Text", StringType(), True),
    StructField("Head.enTitle", StringType(), True),
    StructField("Body.Earthquake.OriginTime", StringType(), True),
    StructField("Body.Earthquake.ArrivalTime", StringType(), True),
    StructField("Body.Earthquake.Hypocenter.Area.Name", StringType(), True),
    StructField("Body.Earthquake.Hypocenter.Area.Code", StringType(), True),
    StructField("Body.Earthquake.Hypocenter.Area.Coordinate", StringType(), True),
    StructField("Body.Earthquake.Hypocenter.Area.enName", StringType(), True),
    StructField("Body.Earthquake.Magnitude", StringType(), True),
    StructField("Body.Intensity.Observation.MaxInt", StringType(), True),
    StructField("Body.Intensity.Observation.Pref", StringType(), True),
    StructField("Body.Comments.ForecastComment.Text", StringType(), True),
    StructField("Body.Comments.ForecastComment.Code", StringType(), True),
    StructField("Body.Comments.ForecastComment.enText", StringType(), True),
    StructField("Body.Comments.VarComment.Text", StringType(), True),
    StructField("Body.Comments.VarComment.Code", StringType(), True),
    StructField("Body.Comments.VarComment.enText", StringType(), True),
    StructField("Head.Headline.Information.Item.Kind.Name", StringType(), True),
    StructField("Head.Headline.Information.Item.Areas.Area", StringType(), True),
    StructField("Body.Earthquake.Hypocenter.Area.DetailedName", StringType(), True),
    StructField("Body.Earthquake.Hypocenter.Area.DetailedCode", StringType(), True),
    StructField("Body.Earthquake.Hypocenter.Source", StringType(), True),
    StructField("Body.Comments.FreeFormComment", StringType(), True),
    StructField("Head.Headline.Information.Item.Areas.Area.Name", StringType(), True),
    StructField("Head.Headline.Information.Item.Areas.Area.Code", StringType(), True),
    StructField("Body.EarthquakeInfo.InfoKind", StringType(), True),
    StructField("Body.EarthquakeInfo.InfoSerial.Name", StringType(), True),
    StructField("Body.EarthquakeInfo.InfoSerial.Code", StringType(), True),
    StructField("Body.EarthquakeInfo.Text", StringType(), True),
    StructField("Body.EarthquakeInfo.Appendix", StringType(), True),
])

# Create earthquake table
@dp.table(
    name='earthquakes'
)
def create_earthquake_table():
    """
    Function to call get_earthquake_events and return Spark DataFrame

    Returns:
        Spark DataFrame containing earthquake data
    """

    # Call helper script
    df = get_earthquake_events()

    # Create spark df with schema
    return spark.createDataFrame(df, schema=EARTHQUAKE_SCHEMA)

# Helper for earthquake events
def get_earthquake_events():
    """
    Function to query JMA quake api and return seismic activity.

    Returns:
        Pandas DataFrame containing earthquake data

    """

    # Request the list of recent quake events
    df_recent_events = pd.DataFrame()
    recent_data_url = "https://www.jma.go.jp/bosai/quake/data/list.json"

    # Send request
    response = requests.get(recent_data_url)
    if response.status_code == 200:
        recent_events_json = response.json()
        df_recent_events = pd.DataFrame(recent_events_json)

    # Parse list of events
    events_list = df_recent_events["json"]

    # DataFrame for all detailed reports
    df_detailed_events = pd.DataFrame()

    # Request JSON details for each quake event
    for event in events_list:

        # Generate url for each event
        url = "https://www.jma.go.jp/bosai/quake/data/" + event

        # Send request
        response = requests.get(url)
        if response.status_code == 200:
            response_json = response.json()
            temp_df = pd.json_normalize(response_json)
            df_detailed_events = pd.concat([df_detailed_events, temp_df], ignore_index=True)

    # Drop problematic columns before saving to parquet - serialization issue
    for col in ["Head.Headline.Information", "Head.Headline.Information.Item"]:
        if col in df_detailed_events.columns:
            df_detailed_events = df_detailed_events.drop([col], axis=1)

    # Return df
    return df_detailed_events