from pyspark import pipelines as dp

# Final earthquake gold table
@dp.table(
    name = 'earthquake_gold'
)
def final_earthquake_events():

    # Read earthquake raw table
    df = spark.read.table('earthquake_silver')

    # Drop nulls
    df_clean = df.dropna(subset=['latitude', 'longitude', 'depth'])

    # Return final df
    return df_clean

# Final weather gold table
@dp.table(
    name = 'weather_gold'
)
def final_weather_table():

    # Read table
    df = spark.read.table("weather_silver")

    # Drop nulls
    df_clean = df.dropna()

    # Return final df
    return df_clean