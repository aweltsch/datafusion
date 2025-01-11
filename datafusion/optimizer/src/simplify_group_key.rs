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

//! [`SimplifyGroupKey`] simplifies the keys used in group by and deduplicates the result

//mod simplify_group_key;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::tree_node::Transformed;
use datafusion_common::{Column, Result};
use datafusion_expr::{Aggregate, Expr, LogicalPlan, LogicalPlanBuilder};
use std::collections::HashSet;

#[derive(Default, Debug)]
pub struct SimplifyGroupKey {}

impl SimplifyGroupKey {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for SimplifyGroupKey {
    fn name(&self) -> &str {
        "simplify_group_key"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Aggregate(aggregate) => {
                let (column_group_expr, non_column_group_expr): (Vec<_>, Vec<_>) =
                    aggregate
                        .group_expr
                        .iter()
                        .partition(|expr| matches!(expr, Expr::Column(_)));

                if column_group_expr.is_empty() {
                    Ok(Transformed::no(LogicalPlan::Aggregate(Aggregate::try_new(
                        aggregate.input,
                        aggregate.group_expr.into_iter().collect(),
                        aggregate.aggr_expr,
                    )?)))
                } else {
                    // FIXME: do I need a projection?
                     let projection_expr: Vec<_> = aggregate.group_expr.iter().chain(aggregate.aggr_expr.iter()).map(|e| e.clone()).collect();
                    let column_set: HashSet<_> = column_group_expr
                        .iter()
                        .map(|col| match col {
                            Expr::Column(column) => column.clone(),
                            _ => panic!("Impossibru"),
                        })
                        .collect();

                    let mut group_expr: Vec<Expr> = column_set
                        .iter()
                        .map(|col| Expr::Column(col.clone()))
                        .collect();
                    for expr in non_column_group_expr {
                        let x = get_input_columns(expr);
                        if let Some(cols) = x {
                            if cols.is_subset(&column_set) {
                                continue;
                            } else {
                                group_expr.push(expr.clone());
                            }
                        } else {
                            group_expr.push(expr.clone());
                        }
                    }
                    let simplified_aggregate = LogicalPlan::Aggregate(
                        Aggregate::try_new(
                            aggregate.input,
                            group_expr,
                            aggregate.aggr_expr,
                        )?);
                 let projection = LogicalPlanBuilder::from(simplified_aggregate).project(projection_expr)?.build()?;
                    Ok(Transformed::yes(projection))
                }
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

/// example GROUP BY col1, col1 + 1, 2*col1 -> col1
fn get_input_columns(expr: &Expr) -> Option<HashSet<Column>> {
    // can be extended
    match expr {
        Expr::Column(c) => Some(HashSet::from([c.clone()])),
        Expr::Literal(_) => Some(HashSet::new()),
        Expr::BinaryExpr(b) => {
            // FIXME: is this the most efficient way?
            let mut x = get_input_columns(b.left.as_ref())?;
            let mut y = get_input_columns(b.right.as_ref())?;
            x.extend(y.drain());
            Some(x)
        }
        Expr::Cast(c) => get_input_columns(c.expr.as_ref()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_common::Result;
    use std::sync::Arc;
    use crate::unwrap_cast_in_comparison::UnwrapCastInComparison;

    use datafusion_expr::{
        col, lit, binary_expr, ColumnarValue, Operator, LogicalPlanBuilder, ScalarUDF, ScalarUDFImpl, Signature,
        TypeSignature,
    };
    use datafusion_functions_aggregate::expr_fn::count;

    #[test]
    fn test_eliminate_superfluous_columns() -> Result<()> {
        let scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![col("a"), binary_expr(col("a"), Operator::Plus, col("a")), binary_expr(col("a"), Operator::Plus, lit(1))], vec![count(col("c"))])?
            .build()?;
        let expected = "\
            Projection: test.a, test.a + test.a, test.a + Int32(1), count(test.c)\
            \n  Aggregate: groupBy=[[test.a]], aggr=[[count(test.c)]]\
            \n    TableScan: test\
        ";
        assert_optimized_plan_eq(Arc::new(SimplifyGroupKey::new()), plan, expected)
    }
}
