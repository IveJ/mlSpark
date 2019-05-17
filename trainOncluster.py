# define a schema for the result set, the user ID and model prediction
schema = StructType([StructField('user_id', LongType(), True),
                     StructField('prediction', DoubleType(), True)])  

# define the Pandas UDF 
@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def apply_model(sample_pd):

    # run the model on the partitioned data set 
    ids = sample_df['user_id']
    x_train = sample_df.drop(['label', 'user_id', 'partition_id'], axis=1)
    pred = model.predict_proba(x_train)

    return pd.DataFrame({'user_id': ids, 'prediction': pred[:,1]})
  
# partition the data and run the UDF  
results = spark_df.groupby('partition_id').apply(apply_model)
display(results)   
