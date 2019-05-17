# mlSpark
Use udf to load dataset, populate cross cluster nodes, sklearn to classify and modelize, train

As long as your complete data set can fit into memory, you can use the single machine approach to model application shown below, to apply the sklearn model to a new data frame. However, if you need to score millions or billions of records, then this single machine approach may fail.


The outcome of this step is a data frame of user IDs and model predictions.


In the last step in the notebook, we’ll use a Pandas UDF to scale the model application process. Instead of pulling the full dataset into memory on the driver node, we can use Pandas UDFs to distribute the dataset across a Spark cluster, and use pyarrow to translate between the spark and Pandas data frame representations. The result is the same as the code snippet above, but in this case the data frame is distributed across the worker nodes in the cluster, and the task is executed in parallel on the cluster.


The result is the same as before, but the computation has now moved from the driver node to a cluster of worker nodes. The input and output of this process is a Spark dataframe, even though we’re using Pandas to perform a task within our UDF.


For more details on setting up a Pandas UDF, check out my prior post on getting up and running with PySpark.

A Brief Introduction to PySpark

PySpark is a great language for performing exploratory data analysis at scale, building machine learning pipelines, and…
towardsdatascience.com	
This was an introduction that showed how to move sklearn processing from the driver node in a Spark cluster to the worker nodes. I’ve also used this functionality to scale up the Featuretools library to work with billions of records and create hundreds of predictive models.
