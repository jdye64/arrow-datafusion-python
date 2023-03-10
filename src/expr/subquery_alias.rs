// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use datafusion_expr::SubqueryAlias;
use pyo3::prelude::*;

use crate::sql::logical::PyLogicalPlan;

use super::logical_node::LogicalNode;

#[pyclass(name = "SubqueryAlias", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PySubqueryAlias {
    subquery_alias: SubqueryAlias,
}

impl From<PySubqueryAlias> for SubqueryAlias {
    fn from(subquery_alias: PySubqueryAlias) -> Self {
        subquery_alias.subquery_alias
    }
}

impl From<SubqueryAlias> for PySubqueryAlias {
    fn from(subquery_alias: SubqueryAlias) -> PySubqueryAlias {
        PySubqueryAlias { subquery_alias }
    }
}

impl LogicalNode for PySubqueryAlias {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![PyLogicalPlan::from((*self.subquery_alias.input).clone())]
    }

    fn to_variant(&self, py: Python) -> PyResult<PyObject> {
        Ok(self.clone().into_py(py))
    }
}
