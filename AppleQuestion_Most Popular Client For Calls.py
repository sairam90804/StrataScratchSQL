

#most pop client_id based on count of users who have 
#50% from events - [video call sent/received, voice call sent/received]

# Import your libraries
import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round

# Start writing code
fact_events

# To validate your solution, convert your final pySpark df to a pandas df
df = fact_events.toPandas()

df.head()
#display the desktop users
desktop_users_df = fact_events.filter(col("client_id") == "desktop")
desktop_users_df.head()

desktop_users_count = fact_events.filter(col("client_id") == "desktop").count()
print("Desktop user count:", desktop_users_count)

mobile_users_df = fact_events.filter(col("client_id") == "mobile")
mobile_users_df.head()

mobile_users_count = fact_events.filter(col("client_id") == "mobile").count()
print("Mobile user count:", mobile_users_count)

client_ids = fact_events.select("client_id").distinct().toPandas()["client_id"].tolist()
print(client_ids)

customer_ids = fact_events.select("customer_id").distinct().toPandas()["customer_id"].tolist()
print(customer_ids)


event_types = fact_events.select("event_type").distinct().toPandas()["event_type"].tolist()

print(event_types)
#Select the most popular client_id based on a count of the number of users who have at least 50% of their events from the following list: 'video call received', 'video call sent', 'voice call received', 'voice call sent'.

#total count _ group users by event types () | then have a group count _ of (specified types) | then divide these two counts where there is either desktop / mobile that turns up / like rank them 

# if this was sql  - select count(users) , evnttype from factevents groupby event types order by
# select count(users) , event type from fact events | 

from pyspark.sql import functions as F
from pyspark.sql.functions import col

special_event_types = ['video call received', 'video call sent', 'voice call received', 'voice call sent']

#Count total events per user
total_events = fact_events.groupBy("user_id").agg(F.count('*').alias('total_events'))

#Count special events per user
special_events = fact_events.filter(col("event_type").isin(special_event_types)) \
    .groupBy("user_id").agg(F.count('*').alias('special_event_types'))

#Join and calculate the special event ratio
user_event_percent = total_events.join(special_events, on="user_id", how="left").fillna(0)

user_event_percent = user_event_percent.withColumn(
    "special_event_ratio", F.col("special_event_types") / F.col("total_events")
)

#Filter users with >= 50% special event ratio
users_df = user_event_percent.filter(F.col('special_event_ratio') >= 0.5).select("user_id")

#Find client type for these users
qualified_user_clients = fact_events.join(users_df, on="user_id", how="inner") \
    .select("user_id", "client_id").dropDuplicates()

#Group by client_id to find popularity
popular_client = qualified_user_clients.groupBy("client_id") \
    .agg(F.countDistinct("user_id").alias("qualified_user_count")) \
    .orderBy(F.desc("qualified_user_count"))


popular_client.show()
