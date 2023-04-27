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

import cudf
import operator
import os
import datafusion
from datafusion.expr import Projection, TableScan, Column, CrossJoin, Filter, Distinct, BinaryExpr, Literal


_OPERATOR_MAPPING = {
    ">=": operator.ge,
    "<=": operator.le,
    "and": operator.and_,
}

class SessionContext:
    def __init__(self):
        self.datafusion_ctx = datafusion.SessionContext()
        self.parquet_tables = {}

    def register_parquet(self, name, path):
        self.parquet_tables[name] = path
        self.datafusion_ctx.register_parquet(name, path)


    def cudf_enable_spilling(self):
        SPILL_FLAG = True
        # 45GB
        CUDF_SPILL_DEVICE_LIMIT = 4.5e+10
        cudf.set_option("spill", SPILL_FLAG)
        cudf.set_option("spill_on_demand", SPILL_FLAG)
        cudf.set_option("spill_device_limit", CUDF_SPILL_DEVICE_LIMIT)
        os.environ["CUDF_SPILL"] = "1"

    def to_cudf_expr(self, expr):
        # get Python wrapper for logical expression
        expr = expr.to_variant()

        print(f"Expr: {expr}")

        if isinstance(expr, Column):
            return expr.name()
        elif isinstance(expr, BinaryExpr):
            # if df is not None:
            #     print(f"Dataframe isn't None")
            #     cudf_op = (self.to_cudf_expr(expr.left(), df), expr.op(), self.to_cudf_expr(expr.right(), df))
            #     # if cudf_op[1] == "AND":
            #     #     breakpoint()
            #     # df = df[_OPERATOR_MAPPING[cudf_op[1].lower()](df[cudf_op[0]], cudf_op[2])]
            #     # print(f"Returning DataFrame after expr: {cudf_op}")
            #     print(f"Operation: {cudf_op}")

            # else:
                return (self.to_cudf_expr(expr.left()), expr.op(), self.to_cudf_expr(expr.right()))
        elif isinstance(expr, Literal):
            match expr.data_type():
                case "Int8":
                    return expr.value_i8()
                case "Int16":
                    return expr.value_i16()
                case "Int32":
                    return expr.value_i32()
                case "Int64":
                    return expr.value_i64()
                case _:
                    print(f"Unable to retrieve Literal value for type: {expr.data_type()}")
        else:
            raise Exception("unsupported expression: {}".format(expr))


    def to_cudf_operation(self, df, expr):
        # get Python wrapper for logical expression
        expr = expr.to_variant()

        if isinstance(expr, Column):
            print(f"Returning series")
            return df[expr.name()]
        elif isinstance(expr, BinaryExpr):
            left = self.to_cudf_operation(df, expr.left())
            op = expr.op()
            right = self.to_cudf_operation(df, expr.right())
            if op.lower() == "and":
                breakpoint()
            return df[_OPERATOR_MAPPING[op.lower()](left, right)]
        elif isinstance(expr, Literal):
            match expr.data_type():
                case "Int8":
                    return expr.value_i8()
                case "Int16":
                    return expr.value_i16()
                case "Int32":
                    return expr.value_i32()
                case "Int64":
                    return expr.value_i64()
                case _:
                    print(f"Unable to retrieve Literal value for type: {expr.data_type()}")
        else:
            raise Exception("unsupported operation: {}".format(expr))

    
    # Converts an Expr(s) instance(s) into a DNF format which can be interpreted by several readers and filtering
    def to_dnf(self, expr):
        # get Python wrapper for logical expression
        expr = expr.to_variant()

        if isinstance(expr, BinaryExpr):
            left = self.to_dnf(expr.left())
            op = expr.op()
            right = self.to_dnf(expr.right())
            breakpoint()
            # return df[_OPERATOR_MAPPING[op.lower()](left, right)]
        elif isinstance(expr, Column):
            return expr.name()
        elif isinstance(expr, Literal):
            match expr.data_type():
                case "Int8":
                    return expr.value_i8()
                case "Int16":
                    return expr.value_i16()
                case "Int32":
                    return expr.value_i32()
                case "Int64":
                    return expr.value_i64()
                case _:
                    print(f"Unable to retrieve Literal value for type: {expr.data_type()}")
        else:
            raise Exception("unsupported DFN operation: {}".format(type(expr)))


    def to_cudf_df(self, plan):
        # recurse down first to translate inputs into cudf data frames
        inputs = [self.to_cudf_df(x) for x in plan.inputs()]

        # get Python wrapper for logical operator node
        node = plan.to_variant()

        if isinstance(node, Projection):
            projections = [self.to_cudf_expr(expr) for expr in node.projections()]
            return inputs[0][projections]

        elif isinstance(node, TableScan):
            # tuple is (column_index, column_name). We want the column_name here
            cols = [tuple[1] for tuple in node.projection()] or None
            filters = [self.to_cudf_expr(expr) for expr in node.filters()] or None
            return cudf.read_parquet(self.parquet_tables[node.table_name()], engine="cudf", columns=cols, filters=filters)
        elif isinstance(node, CrossJoin):
            left = inputs[0]
            right = inputs[1]
            return left.join(right, how="outer", lsuffix='left_', rsuffix="right_", sort=False)
        elif isinstance(node, Filter):
            print(f"Filter")
            df = inputs[0]
            dnf = self.to_dnf(node.predicate())
            breakpoint()

            # The PyExpr instance representing the filter
            # filter_expr = self.to_cudf_expr(node.predicate())
            # df = inputs[0]
            # assert(len(filter_expr) % 3 == 0)
            # filter = "df['{}'] {} {}".format()
            # breakpoint()
            # return filtered_df

            # if isinstance(filter, tuple):
            #     print(f"Tuple")
            #     left = filter[0]
            #     op = filter[1]
            #     right = filter[2]
            #     breakpoint()
            # else:
            #     raise Exception("'{}' is not a Tuple, unsure how to continue?".format(filter))

            # df = inputs[0]
            # breakpoint()
            # # # TODO: Obviously need to perform the actual filtering logic here ...
            # # df = df[(df['ss_sold_date_sk'] == df['d_date_sk']) 
            # #         & (df['ss_customer_sk'] == df['c_customer_sk'])
            # #         & ((df['d_month_seq'] >= 1189) & (df['d_month_seq'] <= 1200))]
            # # return df
            # return inputs[0]
        elif isinstance(node, Distinct):
            print(f"Distinct")
            pass
        else:
            raise Exception(
                "unsupported logical operator: {}".format(type(node))
            )

    def sql(self, sql):
        # self.cudf_enable_spilling()
        datafusion_df = self.datafusion_ctx.sql(sql)
        plan = datafusion_df.logical_plan()
        optimized_plan = self.datafusion_ctx.optimize(plan)
        return self.to_cudf_df(optimized_plan)
