#!/usr/bin/env python
# coding: utf-8

# ## Silver_ShoppingMart_Transformations
# 
# null

# # Silver Layer Notebook: Cleaning and Transformation

# In[ ]:


from pyspark.sql.functions import *

#Input file paths
customers_path = "abfss://ShoppingMart_Analytics@onelake.dfs.fabric.microsoft.com/Bronze_ShoppingMart_Lakehouse.Lakehouse/Files/Bronze_Customers/customers.csv"
orders_path = "abfss://ShoppingMart_Analytics@onelake.dfs.fabric.microsoft.com/Bronze_ShoppingMart_Lakehouse.Lakehouse/Files/Bronze_Orders/orders.csv"
products_path = "abfss://ShoppingMart_Analytics@onelake.dfs.fabric.microsoft.com/Bronze_ShoppingMart_Lakehouse.Lakehouse/Files/Bronze_Products/products.csv"
review_path = "abfss://ShoppingMart_Analytics@onelake.dfs.fabric.microsoft.com/Bronze_ShoppingMart_Lakehouse.Lakehouse/Files/Bronze_Reviews/review.json"
social_media_path = "abfss://ShoppingMart_Analytics@onelake.dfs.fabric.microsoft.com/Bronze_ShoppingMart_Lakehouse.Lakehouse/Files/Bronze_Social_Media/social_media.json"
web_logs_path = "abfss://ShoppingMart_Analytics@onelake.dfs.fabric.microsoft.com/Bronze_ShoppingMart_Lakehouse.Lakehouse/Files/Bronze_Web_Logs/web_logs.json"

#Output tables path
OrderDetails_output_path = "abfss://ShoppingMart_Analytics@onelake.dfs.fabric.microsoft.com/Silver_ShoppingMart_Lakehouse.Lakehouse/Tables/OrderDetails"
review_output_path = "abfss://ShoppingMart_Analytics@onelake.dfs.fabric.microsoft.com/Silver_ShoppingMart_Lakehouse.Lakehouse/Tables/review"
social_media_output_path = "abfss://ShoppingMart_Analytics@onelake.dfs.fabric.microsoft.com/Silver_ShoppingMart_Lakehouse.Lakehouse/Tables/social_media"
web_logs_output_path = "abfss://ShoppingMart_Analytics@onelake.dfs.fabric.microsoft.com/Silver_ShoppingMart_Lakehouse.Lakehouse/Tables/web_logs"


# In[ ]:


df_customers = spark.read.format("csv").option("header","true").option("inferschema","true").load(customers_path)
df_orders = spark.read.format("csv").option("header","true").option("inferschema","true").load(orders_path)
df_products = spark.read.format("csv").option("header","true").option("inferschema","true").load(products_path)

df_review = spark.read.option("multiline", "true").json(review_path)
df_social_media = spark.read.option("multiline", "true").json(social_media_path)
df_web_logs = spark.read.option("multiline", "true").json(web_logs_path)

#display(df_customers)


# ## Cleaning and Transforming the Data

# In[ ]:


df_orders = df_orders.dropna(subset = ["OrderID", "OrderDate", "CustomerID", "ProductID", "TotalAmount"])
df_orders = df_orders.withColumn("OrderDate", to_date(col("OrderDate")))
#display(df_orders)


# In[ ]:


# JOIN ORDERS WITH PRODUCTS & CUSTOMERS

df_finaljoin = df_orders.join(df_customers, on="CustomerID", how="inner")\
                        .join(df_products, on="ProductID", how="inner")

#display(df_finaljoin.limit(3))


# In[ ]:


# Save the DataFrame as a table
# Drop the table using Spark SQL
spark.sql("DROP TABLE IF EXISTS OrderDetails")
spark.sql("DROP TABLE IF EXISTS review")
spark.sql("DROP TABLE IF EXISTS social_media")
spark.sql("DROP TABLE IF EXISTS web_logs")

# WRITE DATA TO SILVER LAYER (We can save to tables in 2 different formats as shown below)
df_finaljoin.write.mode("overwrite").format("delta").save(OrderDetails_output_path)
df_review.write.mode("overwrite").format("delta").save("Tables/review")
df_social_media.write.mode("overwrite").format("delta").save("Tables/social_media")
df_web_logs.write.mode("overwrite").format("delta").save("Tables/web_logs")

