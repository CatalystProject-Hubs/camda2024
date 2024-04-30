import polars as pl

# load initial (and clean) data
df = pl.read_csv("preprocessed_data/eHRs-gen2.csv")

# aggregate by age
df = df\
  .group_by(["p_id","age"])\
  .agg([
    pl.col(col).max().alias(col) for col in df.columns if col not in ["age","p_id"]
  ])

# save the result
df\
  .sort([
    pl.col("p_id").cast(pl.Int32),
    pl.col("age").cast(pl.Int32),
    pl.col("v_o_id").map_elements(lambda s: int(s.split("_")[1]),return_dtype=pl.Int32)
  ])\
  .write_csv("preprocessed_data/eHRs-gen2-aggregated-1y.csv")
pass