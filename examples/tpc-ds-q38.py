# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from datafusion.cudf import SessionContext
import time


ctx = SessionContext()
ctx.register_parquet("store_sales", "/media/jeremy/storage/tpc-ds/sf100/parquet_2gb/store_sales")
ctx.register_parquet("date_dim", "/media/jeremy/storage/tpc-ds/sf100/parquet_2gb/date_dim")
ctx.register_parquet("customer", "/media/jeremy/storage/tpc-ds/sf100/parquet_2gb/customer")
ctx.register_parquet("catalog_sales", "/media/jeremy/storage/tpc-ds/sf100/parquet_2gb/catalog_sales")
ctx.register_parquet("web_sales", "/media/jeremy/storage/tpc-ds/sf100/parquet_2gb/web_sales")

# sql = """
# select  count(*) from (
#     select distinct c_last_name, c_first_name, d_date
#     from store_sales, date_dim, customer
#           where store_sales.ss_sold_date_sk = date_dim.d_date_sk
#       and store_sales.ss_customer_sk = customer.c_customer_sk
#       and d_month_seq between 1189 and 1189 + 11
#   intersect
#     select distinct c_last_name, c_first_name, d_date
#     from catalog_sales, date_dim, customer
#           where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
#       and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
#       and d_month_seq between 1189 and 1189 + 11
#   intersect
#     select distinct c_last_name, c_first_name, d_date
#     from web_sales, date_dim, customer
#           where web_sales.ws_sold_date_sk = date_dim.d_date_sk
#       and web_sales.ws_bill_customer_sk = customer.c_customer_sk
#       and d_month_seq between 1189 and 1189 + 11
# ) hot_cust
# limit 100
# """

# sql = """
# select distinct c_last_name, c_first_name, d_date
#     from store_sales, date_dim, customer
#           where store_sales.ss_sold_date_sk = date_dim.d_date_sk
#       and store_sales.ss_customer_sk = customer.c_customer_sk
#       and d_month_seq between 1189 and 1189 + 11
# intersect
#     select distinct c_last_name, c_first_name, d_date
#         from catalog_sales, date_dim, customer
#             where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
#         and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
#         and d_month_seq between 1189 and 1189 + 11
# """

sql = """
select distinct c_last_name, c_first_name, d_date
    from store_sales, date_dim, customer
          where store_sales.ss_sold_date_sk = date_dim.d_date_sk
      and store_sales.ss_customer_sk = customer.c_customer_sk
      and d_month_seq between 1189 and 1189 + 11
"""

start = time.time()
df = ctx.sql(sql)
run_time = time.time() - start
print(f"Query RunTime: {run_time}")
print(df)
