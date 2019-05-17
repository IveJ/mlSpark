# train a model, but first, pull everything to the driver node
df = spark_df.toPandas().drop(['user_id', 'partition_id'], axis = 1)

y_train = df['label']
x_train = df.drop(['label'], axis=1)

# use logistic regression
model = LogisticRegression()
model.fit(x_train, y_train)

# pull all data to the driver node
sample_df = spark_df.toPandas()

# create a prediction for each user 
ids = sample_df['user_id']
x_train = sample_df.drop(['label', 'user_id', 'partition_id'], axis=1)
pred = model.predict_proba(x_train)
result_df = pd.DataFrame({'user_id': ids, 'prediction': pred[:,1]})

# display the results 
display(spark.createDataFrame(result_df))
