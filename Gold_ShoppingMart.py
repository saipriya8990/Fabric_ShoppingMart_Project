#!/usr/bin/env python
# coding: utf-8

# ## Gold_ShoppingMart
# 
# null

# In[53]:


from pyspark.sql.functions import *


# In[54]:


df_orders = spark.sql("SELECT * FROM Silver_ShoppingMart_Lakehouse.OrderDetails")
df_review = spark.sql("SELECT * FROM Silver_ShoppingMart_Lakehouse.review")
df_social_media = spark.sql("SELECT * FROM Silver_ShoppingMart_Lakehouse.social_media")
df_web_logs = spark.sql("SELECT * FROM Silver_ShoppingMart_Lakehouse.web_logs")
#display(df_web_logs)


# In[55]:


#KPI1: Aggregates product reviews to calculate the average rating per product.
df_review1 = df_review.groupBy("product_id").agg(avg("rating").alias("avgrating"))
df_review1.write\
            .mode("overwrite")\
            .format("delta")\
            .save("Tables/review")

#display(df_review1)


# In[56]:


# KPI2 : Aggregates unstructured social media data to track sentiment trends across different platforms.
df_social_media1 = df_social_media.groupBy("platform", "sentiment").count()
df_social_media1.write\
                .mode("overwrite")\
                .format("delta")\
                .save("Tables/socialmedia")
#display(df_social_media1)


# In[57]:


# KPI3 : Aggregates web log data to measure engagement per user on each page and action.
df_web_logs1 = df_web_logs.groupBy("user_id","page", "action").count()
df_web_logs1.write.mode("overwrite").format("delta").save("Tables/weblogs")
#display(df_web_logs1)


# In[58]:


df_orders.write\
            .mode("overwrite")\
            .format("delta")\
            .save("Tables/orderdetails")


# In[ ]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# --How many orders each customer has purchased based on order date
# select CustomerID, OrderDate, count(OrderID) OrderCount
# from orderdetails 
# Group By CustomerID, OrderDate
# having count(OrderID)>1

