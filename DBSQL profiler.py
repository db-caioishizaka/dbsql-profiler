# Databricks notebook source
dbutils.widgets.text("daysLookback", defaultValue = "7", label = "Days Lookback")
dbutils.widgets.text("targetAutoStop", defaultValue = "10", label = "Target auto stop on serverless (minutes)")

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks SQL profiler
# MAGIC 
# MAGIC ### What does it do?
# MAGIC 
# MAGIC The purpose of this notebook is to make an assessment on the workspaces current Warehouses. It will get all the queries from the past 7 days (customizable) and understand what are the idle times, and if there is an opportunity to bring Serverless in with a more aggressive auto-stop policy
# MAGIC 
# MAGIC ### What is the rationale behind it?
# MAGIC 
# MAGIC Databricks charges for the time clusters/warehouses are on, regardless whether there are queries running or not.
# MAGIC 
# MAGIC Classic SQL works like a conventional cluster, with a start-up time of ~5min (on average). Meaning that too aggressive auto-stop policies can leave users frustrated often times, waiting 5 minutes to be able to run their queries. Typically, warehouses are setup with auto-stop after 60-120 minutes.
# MAGIC 
# MAGIC Serverless SQL has a much faster start-up time (10-15 seconds), which allows for aggressive auto-stop strategy, as users won't be that frustrated if they have to wait for 15 seconds. Auto-stop on Serverless can be as low as 10 minutes (though Databricks will reduce that even further in the near future)
# MAGIC 
# MAGIC ###  How does this analysis work
# MAGIC 
# MAGIC This notebook will hit the Query History API, to grab all query execution for the past 7 days. This window is customizable, but we strongly recommend 7 days to capture any weekday seasonality.
# MAGIC 
# MAGIC With this info in hand, it will define all the intervals in which queries were executing (active intervals). All the intervals that does not have a query running are considered idle intervals.
# MAGIC 
# MAGIC It then will analyze how many of those idle intervals can be optimized with the introduction of Serverless

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Requirements
# MAGIC 
# MAGIC - This notebook must be run by a workspace admin. A non-admin user will only be able to retrieve their own query history
# MAGIC - A single node cluster with 8 GB and 2 cores were enough. Most of the time will be spent hitting the API, which is not computational expensive
# MAGIC - Cells must be run in order
# MAGIC - Last display the end result. We recommend downloading the dataframe and share with your Databricks representative to assist in analyzing the results
# MAGIC 
# MAGIC ### Limitations
# MAGIC - This does not cover for scale up and scale down. This assumes the warehouse has at most one cluster. For Serverless benefits on scaling up and down, other analysis are required
# MAGIC - This is only be able to get information (size and auto-stop) for existing warehouses. If there are queries ran in now deleted warehouses, the script assumes a 120-minutes auto stop for the analysis

# COMMAND ----------

# This is a very greedy approach that expects intervals to be entered in ascending order of lower_bound, one by one, and the universe contains all intervals
# The above will always be true if nothing is changed in the notebook (except for the parameters, that are free to be changed, though we recommend keeping 7 days to capture weekend seasonality)
# This was the easiest way to solve this problem in O(n). A more broad solution could be implemented with a bit more effort for O(nˆ2) and a lot of more effort for O(nlogn)
# Given we sort the intervals beforehand, this global solution may be considered O(nlogn)

class TimeInterval:
  def __init__(self, lower_bound: int = None, upper_bound: int = None):
    if lower_bound is None and upper_bound is None:
      self.args = []
    elif type(lower_bound) != int and upper_bound != int:
      raise TypeError("Inputs must be integers")
    else: 
      self.args = [(lower_bound, upper_bound)]
    
  def union(self, new_interval):
    if self.args == []:
      self.args += new_interval.args
      pass
    last_lower, last_upper = self.args[-1]
    new_lower, new_upper = new_interval.args[0]
    if new_lower < last_upper:
      self.args[-1] = (last_lower, max(last_upper, new_upper))
    else:
      self.args += new_interval.args
      
  def complement(self, universe):
    result = TimeInterval()
    abs_lower, abs_upper = universe.args[0]
    for interval in self.args:
      lower, upper = interval
      result.args.append((abs_lower, lower))
      abs_lower = upper
    result.args.append((abs_lower, abs_upper))
    return result

# COMMAND ----------

# Create interface to Databricks APIs
import requests
import json
from pyspark.sql import Row
from pyspark.sql.functions import from_unixtime, col, lit

class DatabricksClient:
  def __init__(self, host, api_key):
    if host[-1] == "/":
      self.host = host[:-2]
    else:
      self.host = host
    self.api_key = api_key
    
  def _get(self, path, body, headers = dict(), verbose = False):
    headers["Authorization"] = f"Bearer {self.api_key}"
    if verbose:
      print(f"Headers: {json.dumps(headers)}")
      print(f"Body: {json.dumps(body)}")
    return requests.get(f"{self.host}/{path}", headers = headers, data = json.dumps(body))
    
  def get_history(self, max_results = 1000, filter_status = None, filter_user_ids = None, filter_endpoint_ids = None, query_start_time_range_start = None, query_start_time_range_end = None, verbose = False):
    results = []
    has_next_page = True
    body = {"max_results": max_results,
            "filter_by": {"query_start_time_range": {}}
           }
    if filter_status:
      body["filter_by"]["status"] = filter_status
    if filter_user_ids:
      body["filter_by"]["user_ids"] = filter_user_ids
    if filter_endpoint_ids:
      body["filter_by"]["endpoint_ids"] = filter_endpoint_ids
    if query_start_time_range_start:
      body["filter_by"]["query_start_time_range"]["start_time_ms"] = query_start_time_range_start
    if query_start_time_range_end:
      body["filter_by"]["query_start_time_range"]["end_time_ms"] = query_start_time_range_end
    token = None
    
    if verbose:
      counter = 1
    while has_next_page:      
      if token:
        body["page_token"] = token
        if "filter_by" in body:
          del body["filter_by"]
      response = self._get("api/2.0/sql/history/queries", body = body, verbose = verbose)
      if response.status_code != 200:
        raise Exception(f"API returned code {response.status_code}. {json.dumps(response.json())}")
      new_data = response.json()
      has_next_page = new_data["has_next_page"]
      token = new_data["next_page_token"]
      results += new_data["res"]
      if verbose:
        counter += 1
        print(counter)

    
#     return spark.createDataFrame(Row(**x) for x in results)
    return spark.createDataFrame(results).withColumn("query_start_date", from_unixtime(col("query_start_time_ms")/lit(1000), "yyyy-MM-dd"))

  def list_warehouses(self, verbose = False):
    body = dict()
    response = self._get("api/2.0/sql/warehouses", body = body, verbose = verbose)
    return spark.createDataFrame(response.json()["warehouses"])
#     return response
    

# COMMAND ----------

# instantiate interface to Databricks APIs
databricksURL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
myToken = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

# databricks = DatabricksClient("https://e2-demo-field-eng.cloud.databricks.com", dbutils.secrets.get(scope = "mysql_romulo", key = "caio_api_key"))
databricks = DatabricksClient(databricksURL, myToken)

# COMMAND ----------

#get warehouses info
warehouses = databricks.list_warehouses()
warehouses.createOrReplaceTempView("warehouses")

# COMMAND ----------

# Get query history. This is the most time consuming step
from time import time

daysLookback = int(dbutils.widgets.get("daysLookback"))
current_timestamp = int(time()*1000)
start_timestamp = int(current_timestamp - 3600*1000*24*daysLookback)

queries = databricks.get_history(query_start_time_range_start = start_timestamp, query_start_time_range_end = current_timestamp)
queries.createOrReplaceTempView("queries")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT warehouse_id, COUNT(1) AS num_queries FROM queries GROUP BY warehouse_id ORDER BY num_queries DESC

# COMMAND ----------

list_of_warehouses = spark.sql("""SELECT COALESCE(warehouses.id, q.warehouse_id) AS warehouse_id, warehouses.name, warehouses.auto_stop_mins, q.num_queries, warehouses.cluster_size, warehouses.enable_serverless_compute FROM warehouses FULL JOIN (SELECT warehouse_id, count(1) AS num_queries FROM queries GROUP BY warehouse_id) q ON q.warehouse_id = warehouses.id ORDER BY num_queries DESC""").rdd.map(lambda x: {"id": x[0], "name": x[1], "auto_stop_mins": x[2], "num_queries": x[3], "cluster_size": x[4], "enable_serverless_compute": x[5]}).collect()

# COMMAND ----------

#Where the magic happens
from pyspark.sql.functions import col 

results = []

target_auto_terminate = int(dbutils.widgets.get("targetAutoStop"))*60*1000

for warehouse in list_of_warehouses:
  warehouse_id = warehouse['id']
  warehouse_name = warehouse['name']
  list_intervals = (queries.select("query_start_time_ms", "query_end_time_ms")
                           .filter(f"warehouse_id = '{warehouse_id}'")
                           .filter(col("query_end_time_ms").isNotNull())
                           .orderBy(col("query_start_time_ms").asc())
                           .rdd.map(lambda x: (x[0], x[1])).collect())
  list_intervals = [TimeInterval(x[0], x[1]) for x in list_intervals]
  usage_intervals = TimeInterval()
  for interval in list_intervals:
    usage_intervals.union(interval)
  usage_durations = [upper - lower for lower, upper in usage_intervals.args]

  universe = TimeInterval(start_timestamp,current_timestamp)
  idle_intervals = usage_intervals.complement(universe)
  idle_durations = [upper - lower for lower, upper in idle_intervals.args]

  auto_stop = warehouse['auto_stop_mins']
  if auto_stop is None:
    current_auto_terminate = 120*60*1000 #120min
  else:
    current_auto_terminate = auto_stop*60*1000   

  idle_above_target = [x for x in idle_durations if x > target_auto_terminate]
  estimated_uptime = sum(usage_durations) + sum(min(x,current_auto_terminate) for x in idle_durations)
  estimated_uptime_percentage = int(estimated_uptime/(current_timestamp - start_timestamp)*100)
  times_above_target = len(idle_above_target)
  duration_above_target = sum(idle_above_target)  
  estimated_savings = sum(min(x, current_auto_terminate) - target_auto_terminate for x in idle_above_target)
  uptime_reduction_percentage = int(estimated_savings/estimated_uptime*100)
  new_uptime = estimated_uptime - estimated_savings
  new_uptime_percentage = int(new_uptime/(current_timestamp - start_timestamp)*100)
  
  result = warehouse.update({"idle_above_target": idle_above_target,
                             "estimated_uptime": estimated_uptime,
                             "estimated_uptime_percentage": estimated_uptime_percentage,
                             "times_above_target": times_above_target,
                             "duration_above_target": duration_above_target,
                             "estimated_savings": estimated_savings,
                             "uptime_reduction_percentage": uptime_reduction_percentage,
                             "new_uptime": new_uptime,
                             "new_uptime_percentage": new_uptime_percentage})
  print(f"Warehouse: {warehouse_name} ({warehouse_id})")
  print(f"Estimated uptime in the period: {int(estimated_uptime/1000)} seconds ({estimated_uptime_percentage}%)")
  print(f"Number of times the endpoint was idle for longer than 10 minutes in the period: {times_above_target}")
  print(f"Total idle time for periods longer than 10 minutes: {int(duration_above_target/1000)} seconds")
  print(f"Potential time savings: {int(estimated_savings/1000)} seconds")
  print(f"Reduction in uptime: {uptime_reduction_percentage}%")
  print(f"New uptime: {int(new_uptime/1000)} seconds ({new_uptime_percentage}%)")
  print("")


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### How to read the results
# MAGIC 
# MAGIC If all went well, you are seeing a paragraph of results for each warehouse. Here is a quick overview on what each line represents
# MAGIC 
# MAGIC - Warehouse: Name of the Warehouse (id of the warehouse)
# MAGIC - Estimated uptime in the period: This is the sum of all the active intervals and idle intervals under current auto-stop, so warehouse would still be up. In parenthesis, the percentage of the period when it stays up
# MAGIC - Number of times the endpoint was idle for longer than 10 minutes in the period: Number of idle intervals that exceed the target auto-stop window, and represents potential savings
# MAGIC - Total idle time for periods longer than 10 minutes: Sum of the total time of the intervals above
# MAGIC - Potential time savings: Actual potential savings, meaning the total time we could have the warehouse turned off under the target auto-stop configuration. It equals the above time, less the auto-stop time for each interval
# MAGIC - Reduction in uptime: Potential time reduction relative to total uptime
# MAGIC - New uptime: total uptime with the reductions (new percentage of the period it stays up)

# COMMAND ----------

final_results = spark.createDataFrame(list_of_warehouses)

# COMMAND ----------

display(final_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next steps
# MAGIC 
# MAGIC Download the last cell results to a csv and send it to your Databricks Representative to talk more about the results
