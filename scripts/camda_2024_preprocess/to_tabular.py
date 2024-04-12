import json
import polars as pl
import pandas as pd
import pyspark as ps
import time

# Prepare the loggers
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

# Load the JSON file
logger.info("Loading the JSON file")
file = "00_origin_data/unzipped/eHRs-gen2/daae_generated_diabetes_patients_camda_gen2.json"
with open( file, "r") as f:
    data = json.load(f)

# Create patient_id and visit_occurrence_id columns
logger.info("Splitting data into visits")
in_time = time.time()
data = [{
    "p_id": k,
    "v_o_id": f"{k}_{idx}",
    "data": ",".join(vv)}
    for k, v in data.items() for idx, vv in enumerate(v)]
logger.info(f"Columns created in {time.time() - in_time:.3f} seconds")

# Crop the data
if True:
  regs = int(1e2)
  logger.info(f"Cropping the data to {regs} rows from {len(data)} initial rows")
  data = data[:regs]

# Create a DataFrame using Polars
logger.info("Creating a DataFrame using Polars")
in_time = time.time()
df = pl.DataFrame(data)
logger.info(f"DataFrame created in {time.time() - in_time:.3f} seconds")

# Determine the person's sex ================================================
# Retrieve the codes 1111 and 2222
logger.info("Determining the person's sex")
df = df.with_columns([
  pl.col("data")\
    .str.contains(code)\
    .alias(col)
  for code,col in {
    "1111": "is_male",
    "2222": "is_female"
  }.items()
])

# Propagate the values to the whole person
df = df.with_columns([
  pl.col(col)\
    .any()\
    .over("p_id")\
    .alias(col)
  for col in ["is_male","is_female"]
])

# Clean the sex column
df = df.with_columns(
  pl.when(df["is_male"] & ~df["is_female"])\
    .then(pl.lit("M"))\
    .when(df["is_female"] & ~df["is_male"])\
    .then(pl.lit("F"))\
    .otherwise(pl.lit(None))\
    .alias("sex")
)

# remove processed columns and drop unnecessary columns
df = df.with_columns(
  pl.col("data").str.replace("1111|2222","")
).drop(["is_male","is_female"])

# Retrieve the visit's age ==================================================
# Retrieve the codes 9xxx
logger.info("Determining the visit's age")
df = df.with_columns(
  pl.col("data")\
    .str.extract("9([01]\\d{2})")\
    .cast(pl.Int32)\
    .alias("age")
)

# Remove processed ages
df = df.with_columns(
  pl.col("data").str.replace("9[01]\\d{2}","")
)

# Remove null ages
df = df.filter(pl.col("age").is_not_null())

# melt 1111 and 2222 columns into sex column
logger.info("Determining sex column")

df = df.with_columns(
   sex = pl\
    .when((df['1111'] == "T") & (df['2222'] == "T")).then(pl.lit("?"))\
    .when(df['1111'] == "T").then(pl.lit("M"))\
    .when(df['2222'] == "T").then(pl.lit("F"))\
    .otherwise(pl.lit("?"))
)

pass