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

use datafusion_expr::expr::{AggregateFunction, WindowFunction, Sort};
use pyo3::{basic::CompareOp, prelude::*};
use std::convert::{From, Into};

use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion_expr::{col, lit, Cast, Expr, GetIndexedField, Between, Like, BinaryExpr, Case, TryCast, Operator, BuiltinScalarFunction};

use crate::errors::{py_runtime_err, py_type_err};
use crate::expr::aggregate_expr::PyAggregateFunction;
use crate::expr::binary_expr::PyBinaryExpr;
use crate::expr::column::PyColumn;
use crate::expr::literal::PyLiteral;
use datafusion::scalar::ScalarValue;

use self::alias::PyAlias;
use self::bool_expr::{
    PyIsFalse, PyIsNotFalse, PyIsNotNull, PyIsNotTrue, PyIsNotUnknown, PyIsNull, PyIsTrue,
    PyIsUnknown, PyNegative, PyNot,
};
use self::like::{PyILike, PyLike, PySimilarTo};
use self::scalar_variable::PyScalarVariable;

pub mod aggregate;
pub mod aggregate_expr;
pub mod alias;
pub mod analyze;
pub mod between;
pub mod binary_expr;
pub mod bool_expr;
pub mod case;
pub mod cast;
pub mod column;
pub mod cross_join;
pub mod distinct;
pub mod empty_relation;
pub mod exists;
pub mod extension;
pub mod filter;
pub mod grouping_set;
pub mod in_list;
pub mod in_subquery;
pub mod indexed_field;
pub mod join;
pub mod like;
pub mod limit;
pub mod literal;
pub mod logical_node;
pub mod placeholder;
pub mod projection;
pub mod scalar_function;
pub mod scalar_subquery;
pub mod scalar_variable;
pub mod signature;
pub mod sort;
pub mod subquery;
pub mod subquery_alias;
pub mod table_scan;
pub mod union;
pub mod create_memory_table;
pub mod create_view;
pub mod drop_table;

/// A PyExpr that can be used on a DataFrame
#[pyclass(name = "Expr", module = "datafusion.expr", subclass)]
#[derive(Debug, Clone)]
pub struct PyExpr {
    pub(crate) expr: Expr,
}

impl From<PyExpr> for Expr {
    fn from(expr: PyExpr) -> Expr {
        expr.expr
    }
}

impl From<Expr> for PyExpr {
    fn from(expr: Expr) -> PyExpr {
        PyExpr { expr }
    }
}

#[pymethods]
impl PyExpr {
    /// Return the specific expression
    fn to_variant(&self, py: Python) -> PyResult<PyObject> {
        Python::with_gil(|_| match &self.expr {
            Expr::Alias(alias, name) => Ok(PyAlias::new(alias, name).into_py(py)),
            Expr::Column(col) => Ok(PyColumn::from(col.clone()).into_py(py)),
            Expr::ScalarVariable(data_type, variables) => {
                Ok(PyScalarVariable::new(data_type, variables).into_py(py))
            }
            Expr::Literal(value) => Ok(PyLiteral::from(value.clone()).into_py(py)),
            Expr::BinaryExpr(expr) => Ok(PyBinaryExpr::from(expr.clone()).into_py(py)),
            Expr::Not(expr) => Ok(PyNot::new(*expr.clone()).into_py(py)),
            Expr::IsNotNull(expr) => Ok(PyIsNotNull::new(*expr.clone()).into_py(py)),
            Expr::IsNull(expr) => Ok(PyIsNull::new(*expr.clone()).into_py(py)),
            Expr::IsTrue(expr) => Ok(PyIsTrue::new(*expr.clone()).into_py(py)),
            Expr::IsFalse(expr) => Ok(PyIsFalse::new(*expr.clone()).into_py(py)),
            Expr::IsUnknown(expr) => Ok(PyIsUnknown::new(*expr.clone()).into_py(py)),
            Expr::IsNotTrue(expr) => Ok(PyIsNotTrue::new(*expr.clone()).into_py(py)),
            Expr::IsNotFalse(expr) => Ok(PyIsNotFalse::new(*expr.clone()).into_py(py)),
            Expr::IsNotUnknown(expr) => Ok(PyIsNotUnknown::new(*expr.clone()).into_py(py)),
            Expr::Negative(expr) => Ok(PyNegative::new(*expr.clone()).into_py(py)),
            Expr::AggregateFunction(expr) => {
                Ok(PyAggregateFunction::from(expr.clone()).into_py(py))
            }
            other => Err(py_runtime_err(format!(
                "Cannot convert this Expr to a Python object: {:?}",
                other
            ))),
        })
    }


    /// Row expressions, Rex(s), operate on the concept of operands.
    pub fn get_operands(&self) -> PyResult<Vec<PyExpr>> {
        match &self.expr {
            // Expr variants that are themselves the operand to return
            Expr::Column(..) | Expr::ScalarVariable(..) | Expr::Literal(..) => {
                Ok(vec![PyExpr::from(
                    self.expr.clone()
                )])
            }

            // Expr(s) that house the Expr instance to return in their bounded params
            Expr::Alias(expr, ..)
            | Expr::Not(expr)
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsNotUnknown(expr)
            | Expr::Negative(expr)
            | Expr::GetIndexedField(GetIndexedField { expr, .. })
            | Expr::Cast(Cast { expr, .. })
            | Expr::TryCast(TryCast { expr, .. })
            | Expr::Sort(Sort { expr, .. })
            | Expr::InSubquery { expr, .. } => {
                Ok(vec![PyExpr::from(*expr.clone())])
            }

            // Expr variants containing a collection of Expr(s) for operands
            Expr::AggregateFunction(AggregateFunction { args, .. })
            | Expr::AggregateUDF { args, .. }
            | Expr::ScalarFunction { args, .. }
            | Expr::ScalarUDF { args, .. }
            | Expr::WindowFunction(WindowFunction { args, .. }) => Ok(args
                .iter()
                .map(|arg| PyExpr::from(arg.clone()))
                .collect()),

            // Expr(s) that require more specific processing
            Expr::Case(Case {
                expr,
                when_then_expr,
                else_expr,
            }) => {
                let mut operands: Vec<PyExpr> = Vec::new();

                if let Some(e) = expr {
                    operands.push(PyExpr::from(*e.clone()));
                };

                for (when, then) in when_then_expr {
                    operands.push(PyExpr::from(*when.clone()));
                    operands.push(PyExpr::from(*then.clone()));
                }

                if let Some(e) = else_expr {
                    operands.push(PyExpr::from(*e.clone()));
                };

                Ok(operands)
            }
            Expr::InList { expr, list, .. } => {
                let mut operands: Vec<PyExpr> =
                    vec![PyExpr::from(*expr.clone())];
                for list_elem in list {
                    operands.push(PyExpr::from(list_elem.clone()));
                }

                Ok(operands)
            }
            Expr::BinaryExpr(BinaryExpr { left, right, .. }) => Ok(vec![
                PyExpr::from(*left.clone()),
                PyExpr::from(*right.clone()),
            ]),
            Expr::Like(Like { expr, pattern, .. }) => Ok(vec![
                PyExpr::from(*expr.clone()),
                PyExpr::from(*pattern.clone()),
            ]),
            Expr::ILike(Like { expr, pattern, .. }) => Ok(vec![
                PyExpr::from(*expr.clone()),
                PyExpr::from(*pattern.clone()),
            ]),
            Expr::SimilarTo(Like { expr, pattern, .. }) => Ok(vec![
                PyExpr::from(*expr.clone()),
                PyExpr::from(*pattern.clone()),
            ]),
            Expr::Between(Between {
                expr,
                negated: _,
                low,
                high,
            }) => Ok(vec![
                PyExpr::from(*expr.clone()),
                PyExpr::from(*low.clone()),
                PyExpr::from(*high.clone()),
            ]),

            // Currently un-support/implemented Expr types for Rex Call operations
            Expr::GroupingSet(..)
            | Expr::Wildcard
            | Expr::QualifiedWildcard { .. }
            | Expr::ScalarSubquery(..)
            | Expr::Placeholder { .. }
            | Expr::Exists { .. } => Err(py_runtime_err(format!(
                "Unimplemented Expr type: {}",
                self.expr
            ))),
        }
    }


    pub fn get_type(&self) -> PyResult<String> {
        Ok(String::from(match &self.expr {
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op,
                right: _,
            }) => match op {
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
                | Operator::And
                | Operator::Or
                | Operator::IsDistinctFrom
                | Operator::IsNotDistinctFrom
                | Operator::RegexMatch
                | Operator::RegexIMatch
                | Operator::RegexNotMatch
                | Operator::RegexNotIMatch => "BOOLEAN",
                Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Modulo => {
                    "BIGINT"
                }
                Operator::Divide => "FLOAT",
                Operator::StringConcat => "VARCHAR",
                Operator::BitwiseShiftLeft
                | Operator::BitwiseShiftRight
                | Operator::BitwiseXor
                | Operator::BitwiseAnd
                | Operator::BitwiseOr => {
                    // the type here should be the same as the type of the left expression
                    // but we can only compute that if we have the schema available
                    return Err(py_type_err(
                        "Bitwise operators unsupported in get_type".to_string(),
                    ));
                }
            },
            Expr::Literal(scalar_value) => match scalar_value {
                ScalarValue::Boolean(_value) => "Boolean",
                ScalarValue::Float32(_value) => "Float32",
                ScalarValue::Float64(_value) => "Float64",
                ScalarValue::Decimal128(_value, ..) => "Decimal128",
                ScalarValue::Dictionary(..) => "Dictionary",
                ScalarValue::Int8(_value) => "Int8",
                ScalarValue::Int16(_value) => "Int16",
                ScalarValue::Int32(_value) => "Int32",
                ScalarValue::Int64(_value) => "Int64",
                ScalarValue::UInt8(_value) => "UInt8",
                ScalarValue::UInt16(_value) => "UInt16",
                ScalarValue::UInt32(_value) => "UInt32",
                ScalarValue::UInt64(_value) => "UInt64",
                ScalarValue::Utf8(_value) => "Utf8",
                ScalarValue::LargeUtf8(_value) => "LargeUtf8",
                ScalarValue::Binary(_value) => "Binary",
                ScalarValue::LargeBinary(_value) => "LargeBinary",
                ScalarValue::Date32(_value) => "Date32",
                ScalarValue::Date64(_value) => "Date64",
                ScalarValue::Time32Second(_value) => "Time32",
                ScalarValue::Time32Millisecond(_value) => "Time32",
                ScalarValue::Time64Microsecond(_value) => "Time64",
                ScalarValue::Time64Nanosecond(_value) => "Time64",
                ScalarValue::Null => "Null",
                ScalarValue::TimestampSecond(..) => "TimestampSecond",
                ScalarValue::TimestampMillisecond(..) => "TimestampMillisecond",
                ScalarValue::TimestampMicrosecond(..) => "TimestampMicrosecond",
                ScalarValue::TimestampNanosecond(..) => "TimestampNanosecond",
                ScalarValue::IntervalYearMonth(..) => "IntervalYearMonth",
                ScalarValue::IntervalDayTime(..) => "IntervalDayTime",
                ScalarValue::IntervalMonthDayNano(..) => "IntervalMonthDayNano",
                ScalarValue::List(..) => "List",
                ScalarValue::Struct(..) => "Struct",
                ScalarValue::FixedSizeBinary(_, _) => "FixedSizeBinary",
            },
            Expr::ScalarFunction { fun, args: _ } => match fun {
                BuiltinScalarFunction::Abs => "Abs",
                BuiltinScalarFunction::DatePart => "DatePart",
                _ => {
                    return Err(py_type_err(format!(
                        "Catch all triggered for ScalarFunction in get_type; {fun:?}"
                    )))
                }
            },
            Expr::Cast(Cast { expr: _, data_type }) => match data_type {
                DataType::Null => "NULL",
                DataType::Boolean => "BOOLEAN",
                DataType::Int8 | DataType::UInt8 => "TINYINT",
                DataType::Int16 | DataType::UInt16 => "SMALLINT",
                DataType::Int32 | DataType::UInt32 => "INTEGER",
                DataType::Int64 | DataType::UInt64 => "BIGINT",
                DataType::Float32 => "FLOAT",
                DataType::Float64 => "DOUBLE",
                DataType::Timestamp { .. } => "TIMESTAMP",
                DataType::Date32 | DataType::Date64 => "DATE",
                DataType::Time32(..) => "TIME32",
                DataType::Time64(..) => "TIME64",
                DataType::Duration(..) => "DURATION",
                DataType::Interval(..) => "INTERVAL",
                DataType::Binary => "BINARY",
                DataType::FixedSizeBinary(..) => "FIXEDSIZEBINARY",
                DataType::LargeBinary => "LARGEBINARY",
                DataType::Utf8 => "VARCHAR",
                DataType::LargeUtf8 => "BIGVARCHAR",
                DataType::List(..) => "LIST",
                DataType::FixedSizeList(..) => "FIXEDSIZELIST",
                DataType::LargeList(..) => "LARGELIST",
                DataType::Struct(..) => "STRUCT",
                DataType::Union(..) => "UNION",
                DataType::Dictionary(..) => "DICTIONARY",
                DataType::Decimal128(..) => "DECIMAL",
                DataType::Decimal256(..) => "DECIMAL",
                DataType::Map(..) => "MAP",
                _ => {
                    return Err(py_type_err(format!(
                        "Catch all triggered for Cast in get_type; {data_type:?}"
                    )))
                }
            },
            _ => {
                return Err(py_type_err(format!(
                    "Catch all triggered in get_type; {:?}",
                    &self.expr
                )))
            }
        }))
    }


    // pub fn operator_name(&self) -> PyResult<String> {
    //     Ok(match &self.expr {
    //         Expr::BinaryExpr(BinaryExpr {
    //             left: _,
    //             op,
    //             right: _,
    //         }) => format!("{op}"),
    //         Expr::ScalarFunction { fun, args: _ } => format!("{fun}"),
    //         Expr::ScalarUDF { fun, .. } => fun.name.clone(),
    //         Expr::Cast { .. } => "cast".to_string(),
    //         Expr::Between { .. } => "between".to_string(),
    //         Expr::Case { .. } => "case".to_string(),
    //         Expr::IsNull(..) => "is null".to_string(),
    //         Expr::IsNotNull(..) => "is not null".to_string(),
    //         Expr::IsTrue(_) => "is true".to_string(),
    //         Expr::IsFalse(_) => "is false".to_string(),
    //         Expr::IsUnknown(_) => "is unknown".to_string(),
    //         Expr::IsNotTrue(_) => "is not true".to_string(),
    //         Expr::IsNotFalse(_) => "is not false".to_string(),
    //         Expr::IsNotUnknown(_) => "is not unknown".to_string(),
    //         Expr::InList { .. } => "in list".to_string(),
    //         Expr::Negative(..) => "negative".to_string(),
    //         Expr::Not(..) => "not".to_string(),
    //         Expr::Like(Like { negated, .. }) => {
    //             if *negated {
    //                 "not like".to_string()
    //             } else {
    //                 "like".to_string()
    //             }
    //         }
    //         Expr::ILike(Like { negated, .. }) => {
    //             if *negated {
    //                 "not ilike".to_string()
    //             } else {
    //                 "ilike".to_string()
    //             }
    //         }
    //         Expr::SimilarTo(Like { negated, .. }) => {
    //             if *negated {
    //                 "not similar to".to_string()
    //             } else {
    //                 "similar to".to_string()
    //             }
    //         }
    //         _ => {
    //             return Err(py_type_err(format!(
    //                 "Catch all triggered in get_operator_name: {:?}",
    //                 &self.expr
    //             )))
    //         }
    //     })
    // }
    

    // Name of the variant as it would appear in the SQL
    fn variant_name(&self) -> PyResult<String> {
        Ok(self.expr.variant_name().to_string())
    }

    /// Returns the name of this expression as it should appear in a schema. This name
    /// will not include any CAST expressions.
    fn display_name(&self) -> PyResult<String> {
        Ok(self.expr.display_name()?)
    }

    /// Returns a full and complete string representation of this expression.
    fn canonical_name(&self) -> PyResult<String> {
        Ok(self.expr.canonical_name())
    }

    fn __richcmp__(&self, other: PyExpr, op: CompareOp) -> PyExpr {
        let expr = match op {
            CompareOp::Lt => self.expr.clone().lt(other.expr),
            CompareOp::Le => self.expr.clone().lt_eq(other.expr),
            CompareOp::Eq => self.expr.clone().eq(other.expr),
            CompareOp::Ne => self.expr.clone().not_eq(other.expr),
            CompareOp::Gt => self.expr.clone().gt(other.expr),
            CompareOp::Ge => self.expr.clone().gt_eq(other.expr),
        };
        expr.into()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Expr({})", self.expr))
    }

    fn __add__(&self, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok((self.expr.clone() + rhs.expr).into())
    }

    fn __sub__(&self, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok((self.expr.clone() - rhs.expr).into())
    }

    fn __truediv__(&self, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok((self.expr.clone() / rhs.expr).into())
    }

    fn __mul__(&self, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok((self.expr.clone() * rhs.expr).into())
    }

    fn __mod__(&self, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok(self.expr.clone().modulus(rhs.expr).into())
    }

    fn __and__(&self, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok(self.expr.clone().and(rhs.expr).into())
    }

    fn __or__(&self, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok(self.expr.clone().or(rhs.expr).into())
    }

    fn __invert__(&self) -> PyResult<PyExpr> {
        Ok(self.expr.clone().not().into())
    }

    fn __getitem__(&self, key: &str) -> PyResult<PyExpr> {
        Ok(Expr::GetIndexedField(GetIndexedField::new(
            Box::new(self.expr.clone()),
            ScalarValue::Utf8(Some(key.to_string())),
        ))
        .into())
    }

    #[staticmethod]
    pub fn literal(value: ScalarValue) -> PyExpr {
        lit(value).into()
    }

    #[staticmethod]
    pub fn column(value: &str) -> PyExpr {
        col(value).into()
    }

    /// assign a name to the PyExpr
    pub fn alias(&self, name: &str) -> PyExpr {
        self.expr.clone().alias(name).into()
    }

    /// Create a sort PyExpr from an existing PyExpr.
    #[pyo3(signature = (ascending=true, nulls_first=true))]
    pub fn sort(&self, ascending: bool, nulls_first: bool) -> PyExpr {
        self.expr.clone().sort(ascending, nulls_first).into()
    }

    pub fn is_null(&self) -> PyExpr {
        self.expr.clone().is_null().into()
    }

    pub fn cast(&self, to: PyArrowType<DataType>) -> PyExpr {
        // self.expr.cast_to() requires DFSchema to validate that the cast
        // is supported, omit that for now
        let expr = Expr::Cast(Cast::new(Box::new(self.expr.clone()), to.0));
        expr.into()
    }
}

/// Initializes the `expr` module to match the pattern of `datafusion-expr` https://docs.rs/datafusion-expr/latest/datafusion_expr/
pub(crate) fn init_module(m: &PyModule) -> PyResult<()> {
    // expressions
    m.add_class::<PyExpr>()?;
    m.add_class::<PyColumn>()?;
    m.add_class::<PyLiteral>()?;
    m.add_class::<PyBinaryExpr>()?;
    m.add_class::<PyLiteral>()?;
    m.add_class::<PyAggregateFunction>()?;
    m.add_class::<PyNot>()?;
    m.add_class::<PyIsNotNull>()?;
    m.add_class::<PyIsNull>()?;
    m.add_class::<PyIsTrue>()?;
    m.add_class::<PyIsFalse>()?;
    m.add_class::<PyIsUnknown>()?;
    m.add_class::<PyIsNotTrue>()?;
    m.add_class::<PyIsNotFalse>()?;
    m.add_class::<PyIsNotUnknown>()?;
    m.add_class::<PyNegative>()?;
    m.add_class::<PyLike>()?;
    m.add_class::<PyILike>()?;
    m.add_class::<PySimilarTo>()?;
    m.add_class::<PyScalarVariable>()?;
    m.add_class::<alias::PyAlias>()?;
    m.add_class::<scalar_function::PyScalarFunction>()?;
    m.add_class::<scalar_function::PyBuiltinScalarFunction>()?;
    m.add_class::<in_list::PyInList>()?;
    m.add_class::<exists::PyExists>()?;
    m.add_class::<subquery::PySubquery>()?;
    m.add_class::<subquery_alias::PySubqueryAlias>()?;
    m.add_class::<in_subquery::PyInSubquery>()?;
    m.add_class::<scalar_subquery::PyScalarSubquery>()?;
    m.add_class::<placeholder::PyPlaceholder>()?;
    m.add_class::<grouping_set::PyGroupingSet>()?;
    m.add_class::<case::PyCase>()?;
    m.add_class::<cast::PyCast>()?;
    m.add_class::<cast::PyTryCast>()?;
    m.add_class::<between::PyBetween>()?;
    m.add_class::<indexed_field::PyGetIndexedField>()?;
    m.add_class::<extension::PyExtension>()?;
    m.add_class::<distinct::PyDistinct>()?;
    m.add_class::<create_memory_table::PyCreateMemoryTable>()?;
    m.add_class::<create_view::PyCreateView>()?;
    m.add_class::<drop_table::PyDropTable>()?;
    // operators
    m.add_class::<table_scan::PyTableScan>()?;
    m.add_class::<projection::PyProjection>()?;
    m.add_class::<filter::PyFilter>()?;
    m.add_class::<limit::PyLimit>()?;
    m.add_class::<aggregate::PyAggregate>()?;
    m.add_class::<sort::PySort>()?;
    m.add_class::<analyze::PyAnalyze>()?;
    m.add_class::<empty_relation::PyEmptyRelation>()?;
    m.add_class::<join::PyJoin>()?;
    m.add_class::<join::PyJoinType>()?;
    m.add_class::<join::PyJoinConstraint>()?;
    m.add_class::<cross_join::PyCrossJoin>()?;
    m.add_class::<union::PyUnion>()?;
    Ok(())
}
