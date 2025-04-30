# Import your libraries
import pyspark
from pyspark.sql.functions import col, sum as _sum, avg



# Start writing code
airbnb_search_details

#
# To validate your solution, convert your final pySpark df to a pandas df
airbnb_search_details.toPandas()

#first find out what neighbourhoods has beds >= 3
#group by neighbourhood & find total beds in the group 
#only display

#first group by neighbourhood


#to find total number of neighbourhood
from pyspark.sql.functions import col, sum as _sum, avg, round

beds_by_neighbourhood = airbnb_search_details.groupBy("neighbourhood").agg(
    _sum("beds").alias("total_beds"),
    avg("beds").alias("avg_beds")
)
filtered = beds_by_neighbourhood.filter(col("total_beds") >= 3)


result = filtered.select(
    "neighbourhood",
    round(col("avg_beds"), 1).alias("avg_beds")
)
result.show()








