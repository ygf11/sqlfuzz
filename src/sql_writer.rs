// Copyright 2022 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::fuzz_sql::{SQLExpr, SQLJoin, SQLSelect, SQLSubqueryAlias, TableAliasGenerator};
use crate::SQLRelation;
use datafusion::common::Result;

/// Generate a SQL string from a SQLRelation struct
pub fn plan_to_sql(plan: &SQLRelation, indent: usize) -> Result<String> {
    let indent_str = "  ".repeat(indent);
    match plan {
        SQLRelation::Select(SQLSelect {
            projection,
            filter,
            input,
        }) => {
            let expr: Vec<String> = projection
                .iter()
                .map(|e| expr_to_sql(e, indent))
                .collect::<Result<Vec<_>>>()?;
            let input = plan_to_sql(input, indent + 1)?;
            let where_clause = if let Some(predicate) = filter {
                let predicate = expr_to_sql(predicate, indent)?;
                format!("\n{}WHERE {}", indent_str, predicate)
            } else {
                "".to_string()
            };
            Ok(format!(
                "SELECT {}\n{}FROM ({}){}",
                expr.join(", "),
                indent_str,
                input,
                where_clause
            ))
        }
        SQLRelation::TableScan(scan) => Ok(scan.table_name.clone()),
        SQLRelation::Join(SQLJoin {
            left,
            right,
            on,
            join_type,
            ..
        }) => {
            let l = plan_to_sql(left, indent + 1)?;
            let r = plan_to_sql(right, indent + 1)?;
            let join_condition = on
                .iter()
                .map(|(l, r)| format!("{} = {}", l.flat_name(), r.flat_name()))
                .collect::<Vec<_>>()
                .join(" AND ");
            Ok(format!(
                "\n{}({})\n{}{} JOIN\n{}({})\n{}ON {}",
                indent_str,
                l,
                indent_str,
                join_type.to_string().to_uppercase(),
                indent_str,
                r,
                indent_str,
                join_condition
            ))
        }
        SQLRelation::SubqueryAlias(SQLSubqueryAlias { input, alias, .. }) => {
            let sql = plan_to_sql(input, indent + 1)?;
            Ok(format!("({}) {}", sql, alias))
        }
    }
}

/// Generate a SQL string from a SQLRelation struct
pub fn plan_to_sql_alias(
    plan: &SQLRelation,
    indent: usize,
    table_alias_generator: &mut TableAliasGenerator,
) -> Result<String> {
    let indent_str = "  ".repeat(indent);
    match plan {
        SQLRelation::Select(SQLSelect {
            projection,
            filter,
            input,
        }) => {
            let expr: Vec<String> = projection
                .iter()
                .map(|e| expr_to_sql(e, indent))
                .collect::<Result<Vec<_>>>()?;

            let (_, from) = input_to_sql(input, indent + 1, table_alias_generator)?;
            // let input_name = plan_to_sql_alias(input, indent + 1, table_alias_generator)?;
            let where_clause = if let Some(predicate) = filter {
                let predicate = expr_to_sql(predicate, indent)?;
                format!("\n{}WHERE {}", indent_str, predicate)
            } else {
                "".to_string()
            };

            Ok(format!(
                "SELECT {}\n{}FROM {}{}",
                expr.join(", "),
                indent_str,
                from,
                where_clause
            ))
        }
        SQLRelation::TableScan(scan) => Ok(scan.table_name.clone()),
        SQLRelation::Join(SQLJoin {
            left,
            right,
            on,
            join_type,
            ..
        }) => {
            let (l_alias, l) = input_to_sql(left, indent + 1, table_alias_generator)?;
            let (r_alias, r) = input_to_sql(right, indent + 1, table_alias_generator)?;
            println!("l:{}", l);
            println!("r:{}", r);
            // let l = plan_to_sql_alias(left, indent + 1, table_alias_generator)?;
            // let r = plan_to_sql_alias(right, indent + 1, table_alias_generator)?;
            let join_condition = on
                .iter()
                .map(|(l, r)| {
                    format!(
                        "{} = {}",
                        format!("{}.{}", l_alias, l.name),
                        format!("{}.{}", r_alias, r.name)
                    )
                })
                .collect::<Vec<_>>()
                .join(" AND ");
            Ok(format!(
                "\n{}{}\n{}{} JOIN\n{}{}\n{}ON {}",
                indent_str,
                l,
                indent_str,
                join_type.to_string().to_uppercase(),
                indent_str,
                r,
                indent_str,
                join_condition
            ))
        }
        SQLRelation::SubqueryAlias(SQLSubqueryAlias { input, alias, .. }) => {
            let sql = plan_to_sql_alias(input, indent + 1, table_alias_generator)?;
            Ok(format!("({}) {}", sql, alias))
        }
    }
}

/// Generate a SQL string from an expression
fn expr_to_sql(expr: &SQLExpr, indent: usize) -> Result<String> {
    Ok(match expr {
        SQLExpr::Alias { expr, alias } => format!("{} AS {}", expr_to_sql(expr, indent)?, alias),
        SQLExpr::Column(col) => col.flat_name(),
        SQLExpr::BinaryExpr { left, op, right } => {
            let l = expr_to_sql(left, indent)?;
            let r = expr_to_sql(right, indent)?;
            format!("{} {} {}", l, op, r)
        }
        SQLExpr::Exists { subquery, negated } => {
            let sql = plan_to_sql(&SQLRelation::Select(subquery.as_ref().clone()), indent)?;
            if *negated {
                format!("NOT EXISTS ({})", sql)
            } else {
                format!("EXISTS ({})", sql)
            }
        }
    })
}

fn input_to_sql(
    input: &SQLRelation,
    indent: usize,
    table_alias_generator: &mut TableAliasGenerator,
) -> Result<(String, String)> {
    let input_str = plan_to_sql_alias(input, indent + 1, table_alias_generator)?;
    if (*input).is_table_scan() {
        Ok((input_str.clone(), input_str))
    } else {
        let alias = table_alias_generator.next_alias();
        let input_str = format!("({}) AS {}", input_str, alias);
        Ok((alias, input_str))
    }
}
