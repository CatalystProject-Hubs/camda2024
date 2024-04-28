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

# exploding the data
df = df.with_columns(
  pl.col("data")\
    .str.split(",")
).explode("data")

# filter empty data
df = df.filter(pl.col("data") != "")

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

# Report the number of invalid sex values
for row in [
    {"cnd":[1,0], "warn": False, "msg": "No. of male records: "},
    {"cnd":[0,1], "warn": False, "msg": "No. of female records: "},
    {"cnd":[1,1], "warn": False, "msg": "Record with both sex values: "},
    {"cnd":[0,0], "warn": False, "msg": "Record with unknown sex: "}
  ]:
  cnd, warn, msg = row.values()
  # count the number of records
  f1 = df["is_male"] if cnd[0] == 1 else ~df["is_male"]
  f2 = df["is_female"] if cnd[1] == 1 else ~df["is_female"]
  cnt = df.filter(f1 & f2).select("p_id").unique().height
  # report the number of records
  if warn:
    logger.warning(f"{msg}{cnt}")
  else:
     print(f"{' '*21}{msg}{cnt}")

# remove processed columns and drop unnecessary columns
df = df.filter(~pl.col("data").str.contains("1111|2222"))
df = df.drop(["is_male","is_female"])

# Fill null sex with U
df = df.with_columns(
  pl.when(df["sex"].is_null())\
    .then(pl.lit("U"))\
    .otherwise(pl.col("sex"))\
    .alias("sex"))

# Retrieve the visit's age ==================================================
# Retrieve the codes 9xxx
logger.info("Determining the visit's age")
df = df.with_columns(
  pl.col("data")\
    .str.extract("9([01]\\d{2})")\
    .cast(pl.Int32)\
    .alias("_age")
)

# Propagate to the whole visit (keep the first not null value)
df = df.with_columns(
  pl.col("_age")\
    .filter(pl.col("_age").is_not_null())\
    .first()\
    .over("v_o_id")\
    .alias("age")
)

# Report the number of invalid age values
for row in [
  {"cnd": pl.col("age") != pl.col("_age"), "msg": "Records with different age values: "},
  {"cnd": pl.col("age").is_null(), "msg": "Records with unknown age: "}
]:
  cnt = df.filter(row["cnd"]).select("p_id").unique().height
  print(f"{' '*21}{row['msg']}{cnt}")

# Remove processed ages
df = df\
  .filter(pl.col("_age").is_null())\
  .filter(pl.col("age").is_not_null())\
  .drop("_age")

# Retrieve the visit's conditions ===========================================
logger.info("Retrieving the visit's conditions")

# Add values
df = df.with_columns(
  pl.lit("1").alias("value")
)

# add CIE10 column
cie = pl.read_csv("00_origin_data/gen2_CIE10.csv")
cie = cie.select(["CODE_BPS","CIE-10"])
cie = cie.rename({"CODE_BPS":"data","CIE-10":"CIE10"})
# cast and join
df = df.with_columns(pl.col("data").cast(pl.Int64))
df = df.join(cie, on="data", how="left")

# show null CI10 codes
logger.info("Showing null CIE10 codes (if any)")
print(df\
      .filter(pl.col("CIE10").is_null())\
      .select(["data","p_id"])\
      .sort(["data","p_id"])\
      .unique()
      )

# replace null CIE10 codes with data
df = df.with_columns(
  pl.when(df["CIE10"].is_null())\
    .then(df["data"])\
    .otherwise(df["CIE10"])\
    .alias("data")
)

# Impute conditions based on past diagnoses ================================
# get unique visits
_df = df\
  .drop(["data","CIE10","value"])\
  .unique()\
  .with_columns([
    pl.col("v_o_id")\
      .map_elements(lambda s: int(s.split("_")[1]),return_dtype=pl.Int32)\
      .alias("_v_o_id")
  ])
# get alternative df
_df2 = _df.rename({
    "v_o_id": "v_o_id2",
    "age": "age2",
    "_v_o_id": "_v_o_id2"
  })
# join and filter
_df = _df.join(_df2, on=["p_id","sex"], how="left")\
  .with_columns([
    (pl.col("age2") == pl.col("age")).alias("same_age"),
    (pl.col("age2") < pl.col("age")).alias("younger"),
    (pl.col("_v_o_id2") <= pl.col("_v_o_id")).alias("not_newer")
  ])\
  .filter(pl.col("younger") | (pl.col("same_age") & pl.col("not_newer")))\
  .drop(["same_age","younger","not_newer","age2","_v_o_id","_v_o_id2"])

# merge the data
df = df\
  .rename({"v_o_id":"v_o_id2"})\
  .drop("age")\
  .join(_df, on=["p_id","sex","v_o_id2"], how="left")\
  .drop(["v_o_id2"])\
  .unique()

# determine target data ====================================================
logger.info("Determining target data")
targets = pl\
  .read_csv("00_origin_data/gen2_CIE10.csv")\
  .select(["CIE-10","TARGET"])\
  .filter(pl.col("TARGET").is_not_null())\
  .unique()\
  .to_dict()["CIE-10"]\
  .to_list()

# get target diagnoses
_df = df\
  .filter(pl.col("CIE10").is_in(targets))\
  .drop(["v_o_id"])\
  .with_columns([
    ("pred1y_"+pl.col("CIE10")).alias("CIE10"),
    ("pred1y_"+pl.col("data")).alias("data")
  ])

# propagate the prediction to current and past year
_df = _df\
  .with_columns([
    (pl.col("age")-1).alias("age")
  ]).vstack(_df)\
  .unique()

# retrieve the target v_o_id
_df = df\
  .select(["p_id","v_o_id","age"])\
  .join(_df, on=["p_id","age"], how="inner")\
  .unique()

# merge both dataframes
df = df.vstack(_df.select(df.columns))

# pivot the data
logger.info("Pivoting the DataFrame")
df = df.pivot(
  values="value",
  index=["p_id","v_o_id","sex","age"],
  columns="data"
)

# Final logs ===============================================================
logger.info("Sorting the DataFrame")
df = df.sort([
  pl.col("p_id").cast(pl.Int32),
  pl.col("age").cast(pl.Int32),
  pl.col("v_o_id").map_elements(lambda s: int(s.split("_")[1]),return_dtype=pl.Int32)
])
logger.info("Saving the DataFrame")
os.makedirs("preprocessed_data", exist_ok=True)
df.write_csv("preprocessed_data/eHRs-gen2.csv")
logger.info("DataFrame saved")
pass