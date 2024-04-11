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
logger.info("Creating patient_id and visit_occurrence_id columns")
data = [{
    "p_id": k,
    "v_o_id": f"{k}_{idx}",
    **{
    k: "T" for k in set(vv)
    }}
    for k, v in data.items() for idx, vv in enumerate(v)]

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

# Propagate 1111 and 2222 columns by person if
logger.info("Propagating 1111 and 2222 columns by person")
df = df.with_columns({
    "1111": pl.first("1111"),
    "2222": pl.first("2222")
})

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