import polars as pl

# load initial (and clean) data
df = pl.read_csv("preprocessed_data/eHRs-gen2.csv")

# aggregate by age
df = df\
  .group_by(["p_id","age"])\
  .agg([
    pl.col(col).max().alias(col) for col in df.columns if col not in ["age","p_id"]
  ])

# Estimate the years with diabetes (E10-E14)
#   - if no diabetes, return 0
#   - if diabetes, return the number of years with diabetes
df = df\
  .with_columns(
    pl.when( pl.col("E10-E14").is_not_null())\
      .then( pl.col("age"))\
      .otherwise(pl.lit(None))\
      .cast(pl.Int32)\
      .alias("age_at_diabetes")
  )
# propagate min age at diabetes by person
df = df\
  .with_columns(
    pl.col("age_at_diabetes")\
      .min().over("p_id")\
      .alias("age_at_diabetes")
  )
# estimate the years_with_diabetes
df = df\
  .with_columns(
    pl.when(pl.col("age_at_diabetes").is_null())\
      .then(pl.lit(0))\
      .otherwise(pl.col("age") - pl.col("age_at_diabetes"))\
      .alias("years_with_diabetes")
  ).with_columns(
    pl.when(pl.col("years_with_diabetes") < 0)\
      .then(pl.lit(0))\
      .otherwise(pl.col("years_with_diabetes"))\
      .alias("years_with_diabetes")
  ).drop(["age_at_diabetes"])

# save the result
df\
  .sort([
    pl.col("p_id").cast(pl.Int32),
    pl.col("age").cast(pl.Int32),
    pl.col("v_o_id").map_elements(lambda s: int(s.split("_")[1]),return_dtype=pl.Int32)
  ])\
  .write_csv("preprocessed_data/eHRs-gen2-aggregated-1y.csv")
pass