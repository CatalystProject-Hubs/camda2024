import json
import polars as pl
import time
import os

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
if False:
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

# Retrieve repeated ages
df = df.with_columns(
  pl.col("data")\
    .str.extract("9([01]\\d{2})")\
    .cast(pl.Int32)\
    .alias("age_repeat")
)

# remove rows with age_repeat
logger.warn(f"rows removed due to a second age defined: {df.filter(pl.col('age_repeat').is_not_null()).height}")
df = df.filter(pl.col("age_repeat").is_null())

# Remove null ages
logger.warn(f"rows removed due to null age: {df.filter(pl.col('age').is_null()).height}")
df = df.filter(pl.col("age").is_not_null())

# Retrieve the visit's conditions ===========================================
logger.info("Retrieving the visit's conditions")
df = df.with_columns(
  pl.col("data")\
    .str.split(",")
).explode("data")

# remove rows where data is empty
df = df.filter(pl.col("data") != "")

# Add values
df = df.with_columns(
  pl.lit("Y").alias("value")
)

# add CIE10 column
cie = pl.read_csv("preprocessed_data/gen2_CIE10.csv")
cie = cie.select(["CODE_BPS","CIE-10"])
cie = cie.rename({"CODE_BPS":"data","CIE-10":"CIE10"})
# cast and join
df = df.with_columns(pl.col("data").cast(pl.Int64))
df = df.join(cie, on="data", how="left")

# show null CI10 codes
logger.info("Showing null CIE10 codes")
print(df.filter(pl.col("CIE10").is_null()))

# replace null CIE10 codes with data
df = df.with_columns(
  pl.when(df["CIE10"].is_null())\
    .then(df["data"])\
    .otherwise(df["CIE10"])\
    .alias("data")
)

# pivot the data
df = df.pivot(
  values="value",
  index=["p_id","v_o_id","sex","age"],
  columns="data"
)

# Final logs ===============================================================
logger.info("Saving the DataFrame")
os.makedirs("preprocessed_data", exist_ok=True)
df.write_csv("preprocessed_data/eHRs-gen2.csv")
logger.info("DataFrame saved")
pass