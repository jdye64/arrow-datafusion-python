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

use std::fmt::{self, Display, Formatter};

use datafusion_expr::{logical_plan::Repartition, Partitioning};
use pyo3::prelude::*;

use crate::sql::logical::PyLogicalPlan;

use super::logical_node::LogicalNode;

#[pyclass(name = "Repartition", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyRepartition {
    repartition: Repartition,
}

#[pyclass(name = "Partitioning", module = "datafusion.expr", subclass)]
#[derive(Clone)]
pub struct PyPartitioning {
    partitioning: Partitioning,
}

impl From<PyPartitioning> for Partitioning {
    fn from(partitioning: PyPartitioning) -> Self {
        partitioning.partitioning
    }
}

impl From<Partitioning> for PyPartitioning {
    fn from(partitioning: Partitioning) -> Self {
        PyPartitioning { partitioning }
    }
}

impl From<PyRepartition> for Repartition {
    fn from(repartition: PyRepartition) -> Self {
        repartition.repartition
    }
}

impl From<Repartition> for PyRepartition {
    fn from(repartition: Repartition) -> PyRepartition {
        PyRepartition { repartition }
    }
}

impl Display for PyRepartition {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Repartition
            input: {:?}
            partitioning_scheme: {:?}",
            &self.repartition.input, &self.repartition.partitioning_scheme,
        )
    }
}

#[pymethods]
impl PyRepartition {
    fn input(&self) -> PyResult<Vec<PyLogicalPlan>> {
        Ok(Self::inputs(self))
    }

    fn partitioning_scheme(&self) -> PyResult<PyPartitioning> {
        Ok(PyPartitioning {
            partitioning: self.repartition.partitioning_scheme.clone(),
        })
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Repartition({})", self))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("Repartition".to_string())
    }
}

impl LogicalNode for PyRepartition {
    fn inputs(&self) -> Vec<PyLogicalPlan> {
        vec![PyLogicalPlan::from((*self.repartition.input).clone())]
    }

    fn to_variant(&self, py: Python) -> PyResult<PyObject> {
        Ok(self.clone().into_py(py))
    }
}
