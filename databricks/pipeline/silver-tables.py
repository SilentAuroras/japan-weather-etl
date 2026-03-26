from pyspark import pipelines as dp
from pyspark.sql import types as T
from pyspark.sql import functions as F

# Define schema for earthquake silver tables
EARTHQUAKE_SCHEMA_SILVER = T.StructType([
    T.StructField("latitude", T.DoubleType(), True),
    T.StructField("longitude", T.DoubleType(), True),
    T.StructField("depth", T.DoubleType(), True),
    T.StructField("hypocenterName", T.StringType(), True),
    T.StructField("magnitude", T.DoubleType(), True),
    T.StructField("originTime",T.TimestampType(),nullable=True),
])

# Silver table to clean up earthquake data
@dp.table(
    name = 'earthquake_silver',
    schema = EARTHQUAKE_SCHEMA_SILVER
)
def clean_earthquake_events():

    # Read earthquake raw table
    df = spark.read.table('earthquakes')

    # Rename columns to readable column names
    df = df.withColumnRenamed('Body.Earthquake.Hypocenter.Area.Coordinate', 'coordinate') \
           .withColumnRenamed('Body.Earthquake.Hypocenter.Area.enName', 'hypocenterName') \
           .withColumnRenamed('Body.Earthquake.Magnitude', 'magnitude') \
           .withColumnRenamed('Body.Earthquake.OriginTime', 'originTime')\
           .withColumn('magnitude', F.col('magnitude').cast(T.DoubleType()))\
           .withColumn('originTime', F.col('originTime').cast(T.TimestampType()))

    # Drop na on coordinate column
    df_cleaned = df.dropna(subset=['coordinate'])

    # Split the coordinate column - (+32.3+130.4-10000/) - lat+long+depth
    df_cleaned = df_cleaned\
        .withColumn('latitude',F.regexp_extract('coordinate', r'([+-]\d+\.?\d*)', 1).cast(T.DoubleType()))\
        .withColumn('longitude', F.regexp_extract('coordinate', r'[+-]\d+\.?\d*([+-]\d+\.?\d*)', 1).cast(T.DoubleType()))\
        .withColumn('depth', F.regexp_extract('coordinate', r'[+-]\d+\.?\d*[+-]\d+\.?\d*([+-]\d+\.?\d*)', 1).cast(T.DoubleType()))

    # Define columns to keep
    columns = ['latitude', 'longitude', 'depth', 'hypocenterName', 'magnitude', 'originTime']

    # Return filtered df
    return df_cleaned.select(columns)