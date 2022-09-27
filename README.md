# dbsql-profiler
Profiler to assess how DBSQL is being used in terms of utilization

### What does it do?

The purpose of this notebook is to make an assessment on the workspaces current Warehouses. It will get all the queries from the past 7 days (customizable) and understand what are the idle times, and if there is an opportunity to bring Serverless in with a more aggressive auto-stop policy

### What is the rationale behind it?

Databricks charges for the time clusters/warehouses are on, regardless whether there are queries running or not.

Classic SQL works like a conventional cluster, with a start-up time of ~5min (on average). Meaning that too aggressive auto-stop policies can leave users frustrated often times, waiting 5 minutes to be able to run their queries. Typically, warehouses are setup with auto-stop after 60-120 minutes.

Serverless SQL has a much faster start-up time (10-15 seconds), which allows for aggressive auto-stop strategy, as users won't be that frustrated if they have to wait for 15 seconds. Auto-stop on Serverless can be as low as 10 minutes (though Databricks will reduce that even further in the near future)

###  How does this analysis work

This notebook will hit the Query History API, to grab all query execution for the past 7 days. This window is customizable, but we strongly recommend 7 days to capture any weekday seasonality.

With this info in hand, it will define all the intervals in which queries were executing (active intervals). All the intervals that does not have a query running are considered idle intervals.

It then will analyze how many of those idle intervals can be optimized with the introduction of Serverless

### Requirements

- This notebook must be run by a workspace admin. A non-admin user will only be able to retrieve their own query history
- A single node cluster with 8 GB and 2 cores were enough. Most of the time will be spent hitting the API, which is not computational expensive
- Cells must be run in order
- Last display the end result. We recommend downloading the dataframe and share with your Databricks representative to assist in analyzing the results

### Limitations

- This does not cover for scale up and scale down. This assumes the warehouse has at most one cluster. For Serverless benefits on scaling up and down, other analysis are required
- This is only be able to get information (size and auto-stop) for existing warehouses. If there are queries ran in now deleted warehouses, the script assumes a 120-minutes auto stop for the analysis

### How to read the results

If all went well, you are seeing a paragraph of results for each warehouse. Here is a quick overview on what each line represents

- Warehouse: Name of the Warehouse (id of the warehouse)
- Estimated uptime in the period: This is the sum of all the active intervals and idle intervals under current auto-stop, so warehouse would still be up. In parenthesis, the percentage of the period when it stays up
- Number of times the endpoint was idle for longer than 10 minutes in the period: Number of idle intervals that exceed the target auto-stop window, and represents potential savings
- Total idle time for periods longer than 10 minutes: Sum of the total time of the intervals above
- Potential time savings: Actual potential savings, meaning the total time we could have the warehouse turned off under the target auto-stop configuration. It equals the above time, less the auto-stop time for each interval
- Reduction in uptime: Potential time reduction relative to total uptime
- New uptime: total uptime with the reductions (new percentage of the period it stays up)