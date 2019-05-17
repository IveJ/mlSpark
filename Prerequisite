# load pandas, sklearn, and pyspark types and functions
import pandas as pd
from sklearn.linear_model import LogisticRegression
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *

# load the CSV as a Spark data frame
pandas_df = pd.read_csv(
     "https://github.com/bgweber/Twitch/raw/master/Recommendations/games-expand.csv")
spark_df = spark.createDataFrame(pandas_df)

# assign a user ID and a partition ID using Spark SQL
spark_df.createOrReplaceTempView("spark_df")
spark_df = spark.sql("""
select *, user_id%10 as partition_id 
from (
  select *, row_number() over (order by rand()) as user_id
  from spark_df
) 
""")

# preview the results
display(spark_df)
