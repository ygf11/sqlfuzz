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

use async_trait::async_trait;
use datafusion::logical_plan::Subquery;
use datafusion::{
    arrow::datatypes::SchemaRef,
    common::{Column, DFField, DFSchema, DFSchemaRef, Result},
    datasource::{DefaultTableSource, TableProvider, TableType},
    execution::context::SessionState,
    logical_expr::{
        logical_plan::{
            Filter, Join, JoinConstraint, JoinType, Projection, SubqueryAlias, TableScan,
        },
        Expr, ExprSchemable, LogicalPlan, Operator,
    },
    physical_plan::ExecutionPlan,
};
use rand::rngs::ThreadRng;
use rand::Rng;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Default, Debug)]
pub struct TableAliasGenerator {
    next_index: usize,
}

impl TableAliasGenerator {
    pub fn next_alias(&mut self) -> String {
        let idx = self.next_index;
        self.next_index += 1;
        format!("t{}", idx)
    }
}

pub struct SQLTable {
    name: String,
    schema: DFSchema,
}

impl SQLTable {
    pub fn new(name: &str, schema: DFSchema) -> Self {
        Self {
            name: name.to_owned(),
            schema,
        }
    }
}

#[derive(Clone)]
pub enum SQLExpr {
    Alias {
        expr: Box<SQLExpr>,
        alias: String,
    },
    Column(Column),
    BinaryExpr {
        left: Box<SQLExpr>,
        op: Operator,
        right: Box<SQLExpr>,
    },
    Exists {
        subquery: Box<SQLSelect>,
        negated: bool,
    },
}

impl SQLExpr {
    /// Convert SQLExpr to DataFusion logical expression
    pub fn to_expr(&self) -> Result<Expr> {
        match self {
            Self::Alias { expr, alias } => {
                Ok(Expr::Alias(Box::new(expr.to_expr()?), alias.to_string()))
            }
            Self::Column(col) => Ok(Expr::Column(col.clone())),
            Self::BinaryExpr { left, op, right } => Ok(Expr::BinaryExpr {
                left: Box::new(left.to_expr()?),
                op: *op,
                right: Box::new(right.to_expr()?),
            }),
            Self::Exists { subquery, negated } => {
                let x = SQLRelation::Select(subquery.as_ref().clone());
                Ok(Expr::Exists {
                    subquery: Subquery {
                        subquery: Arc::new(x.to_logical_plan()?),
                    },
                    negated: *negated,
                })
            }
        }
    }
}

#[derive(Clone)]
pub struct SQLSelect {
    pub projection: Vec<SQLExpr>,
    pub filter: Option<SQLExpr>,
    pub input: Box<SQLRelation>,
    // pub schema: DFSchemaRef,
}

#[derive(Clone)]
pub struct SQLJoin {
    pub left: Box<SQLRelation>,
    pub right: Box<SQLRelation>,
    pub on: Vec<(Column, Column)>,
    pub filter: Option<SQLExpr>,
    pub join_type: JoinType,
    pub join_constraint: JoinConstraint,
    pub schema: DFSchemaRef,
}

#[derive(Clone)]
pub struct SQLSubqueryAlias {
    pub input: Box<SQLRelation>,
    pub alias: String,
    pub schema: DFSchemaRef,
}

#[derive(Clone)]
pub enum SQLRelation {
    Select(SQLSelect),
    Join(SQLJoin),
    SubqueryAlias(SQLSubqueryAlias),
    TableScan(TableScan),
}

impl SQLRelation {
    pub fn schema(&self) -> DFSchemaRef {
        self.to_logical_plan().unwrap().schema().clone()
    }

    pub fn to_logical_plan(&self) -> Result<LogicalPlan> {
        Ok(match self {
            Self::Select(SQLSelect {
                projection,
                filter,
                input,
                ..
            }) => {
                let input = Arc::new(input.to_logical_plan()?);
                let input_schema = input.schema();

                let expr: Vec<Expr> = projection
                    .iter()
                    .map(|e| e.to_expr())
                    .collect::<Result<Vec<_>>>()?;

                let fields = expr
                    .iter()
                    .map(|e| e.to_field(input_schema))
                    .collect::<Result<Vec<_>>>()?;

                let schema = Arc::new(DFSchema::new_with_metadata(fields, HashMap::new())?);

                let projection = LogicalPlan::Projection(Projection {
                    expr,
                    input,
                    schema,
                    alias: None,
                });
                if let Some(predicate) = filter {
                    LogicalPlan::Filter(Filter {
                        predicate: predicate.to_expr()?,
                        input: Arc::new(projection),
                    })
                } else {
                    projection
                }
            }
            Self::Join(SQLJoin {
                left,
                right,
                on,
                filter,
                join_type,
                join_constraint,
                schema,
            }) => {
                let filter = match filter {
                    Some(f) => Some(f.to_expr()?),
                    _ => None,
                };

                LogicalPlan::Join(Join {
                    left: Arc::new(left.to_logical_plan()?),
                    right: Arc::new(right.to_logical_plan()?),
                    on: on.clone(),
                    filter,
                    join_type: *join_type,
                    join_constraint: *join_constraint,
                    schema: schema.clone(),
                    null_equals_null: false,
                })
            }
            Self::SubqueryAlias(SQLSubqueryAlias {
                input,
                alias,
                schema,
            }) => LogicalPlan::SubqueryAlias(SubqueryAlias {
                input: Arc::new(input.to_logical_plan()?),
                alias: alias.clone(),
                schema: schema.clone(),
            }),
            Self::TableScan(x) => LogicalPlan::TableScan(x.clone()),
        })
    }

    pub fn is_table_scan(&self) -> bool {
        matches!(self, Self::TableScan(_))
    }
}

#[derive(Debug, Clone)]
pub struct FuzzConfig {
    /// Specify which join types should be used in generated queries. Leave empty to disable joins.
    pub join_types: Vec<JoinType>,
    /// Maximum query plan tree depth
    pub max_depth: usize,
}

/// Generates random logical plans
pub struct SQLRelationGenerator<'a> {
    tables: Vec<SQLTable>,
    rng: &'a mut ThreadRng,
    id_gen: usize,
    depth: usize,
    config: FuzzConfig,
    semi_join: bool,
    anti_join: bool,
    column_alias_prefix: String,
}

impl<'a> SQLRelationGenerator<'a> {
    pub fn new(rng: &'a mut ThreadRng, tables: Vec<SQLTable>, config: FuzzConfig) -> Self {
        let semi_join = config.join_types.contains(&JoinType::Semi);
        let anti_join = config.join_types.contains(&JoinType::Anti);
        let mut config = config;
        config.join_types = config
            .join_types
            .iter()
            .cloned()
            .filter(|j| *j != JoinType::Anti && *j != JoinType::Semi)
            .collect();
        Self {
            tables,
            rng,
            id_gen: 0,
            depth: 0,
            config,
            semi_join,
            anti_join,
            column_alias_prefix: "_c".to_string(),
        }
    }

    pub fn generate_select(&mut self) -> Result<SQLRelation> {
        let input = self.generate_relation()?;
        let columns: Vec<Column> = input
            .to_logical_plan()?
            .schema()
            .fields()
            .iter()
            .map(|f| f.qualified_column())
            .collect();

        let projection = columns
            .iter()
            .map(|col| SQLExpr::Column(col.clone()))
            .collect();

        let filter = match self.rng.gen_range(0..3) {
            0 => Some(self.generate_predicate(&input)?),
            1 if self.semi_join => Some(self.generate_exists(false, &columns)?),
            2 if self.anti_join => Some(self.generate_exists(true, &columns)?),
            _ => None,
        };
        Ok(SQLRelation::Select(SQLSelect {
            projection,
            filter,
            input: Box::new(input.clone()),
        }))
    }

    /// Generate uncorrelated subquery in the form `EXISTS (SELECT semi_join_field FROM
    /// semi_join_table)`
    fn generate_exists(&mut self, negated: bool, outer_columns: &[Column]) -> Result<SQLExpr> {
        let semi_join_table = self.generate_select()?;
        let x = semi_join_table.schema();
        let semi_join_field = x.field(self.rng.gen_range(0..x.fields().len()));

        let semi_join_schema = semi_join_table.schema();
        let inner_field =
            semi_join_schema.field(self.rng.gen_range(0..semi_join_schema.fields().len()));
        let outer_field = &outer_columns[self.rng.gen_range(0..outer_columns.len())];

        let filter = SQLExpr::BinaryExpr {
            left: Box::new(SQLExpr::Column(Column::from_qualified_name(
                &inner_field.qualified_name(),
            ))),
            op: Operator::Eq,
            right: Box::new(SQLExpr::Column(outer_field.clone())),
        };

        let subquery = Box::new(SQLSelect {
            projection: vec![SQLExpr::Column(semi_join_field.qualified_column())],
            input: Box::new(semi_join_table),
            filter: Some(filter),
            // schema: Arc::new(DFSchema::new_with_metadata(
            //     vec![semi_join_field.clone()],
            //     HashMap::new(),
            // )?),
        });
        Ok(SQLExpr::Exists { subquery, negated })
    }

    fn generate_predicate(&mut self, input: &SQLRelation) -> Result<SQLExpr> {
        let l = self.get_random_field(input.schema().as_ref());
        let r = self.get_random_field(input.schema().as_ref());
        let op = match self.rng.gen_range(0..6) {
            0 => Operator::Lt,
            1 => Operator::LtEq,
            2 => Operator::Gt,
            3 => Operator::GtEq,
            4 => Operator::Eq,
            5 => Operator::NotEq,
            _ => unreachable!(),
        };
        Ok(SQLExpr::BinaryExpr {
            left: Box::new(SQLExpr::Column(l.qualified_column())),
            op,
            right: Box::new(SQLExpr::Column(r.qualified_column())),
        })
    }

    fn get_random_field(&mut self, schema: &DFSchema) -> DFField {
        let i = self.rng.gen_range(0..schema.fields().len());
        let field = schema.field(i);
        field.clone()
    }

    pub fn generate_relation(&mut self) -> Result<SQLRelation> {
        if self.depth == self.config.max_depth {
            // generate a leaf node to prevent us recursing forever
            self.generate_table_scan()
        } else {
            self.depth += 1;
            let plan = match self.rng.gen_range(0..4) {
                0 if !self.config.join_types.is_empty() => self.generate_join(),
                _ => self.generate_table_scan(),
                //2 => self.generate_select(),
            };
            self.depth -= 1;
            plan
        }
    }

    fn generate_column_alias(&mut self) -> String {
        let id = self.id_gen;
        self.id_gen += 1;
        format!("{}{}", &self.column_alias_prefix, id)
    }

    pub fn select_star(&mut self, plan: &SQLRelation) -> SQLRelation {
        let fields = plan.schema().fields().clone();
        let projection = fields
            .iter()
            .map(|f| SQLExpr::Alias {
                expr: Box::new(SQLExpr::Column(f.qualified_column())),
                alias: self.generate_column_alias(),
            })
            .collect();
        SQLRelation::Select(SQLSelect {
            projection,
            filter: None,
            input: Box::new(plan.clone()),
        })
    }

    fn generate_join(&mut self) -> Result<SQLRelation> {
        let t1 = self.generate_relation()?;
        let t2 = self.generate_relation()?;
        let schema = t1.schema().as_ref().clone();
        schema.join(&t2.schema())?;

        // TODO generate multicolumn joins
        let on = vec![(
            t1.schema().fields()[self.rng.gen_range(0..t1.schema().fields().len())]
                .qualified_column(),
            t2.schema().fields()[self.rng.gen_range(0..t2.schema().fields().len())]
                .qualified_column(),
        )];

        let join_type =
            &self.config.join_types[self.rng.gen_range(0..self.config.join_types.len())];

        Ok(SQLRelation::Join(SQLJoin {
            left: Box::new(t1),
            right: Box::new(t2),
            on,
            filter: None,
            join_type: *join_type,
            join_constraint: JoinConstraint::On,
            schema: Arc::new(schema),
        }))
    }

    fn generate_table_scan(&mut self) -> Result<SQLRelation> {
        let table = self.random_table();
        let projected_schema = Arc::new(table.schema.clone());
        let table_scan = SQLRelation::TableScan(TableScan {
            table_name: table.name.clone(),
            source: Arc::new(DefaultTableSource {
                table_provider: Arc::new(FakeTableProvider {}),
            }),
            projection: None,
            projected_schema,
            filters: vec![],
            fetch: None,
        });
        Ok(self.select_star(&table_scan))
    }

    fn random_table(&mut self) -> &SQLTable {
        &self.tables[self.rng.gen_range(0..self.tables.len())]
    }
}

struct FakeTableProvider {}

#[async_trait]
impl TableProvider for FakeTableProvider {
    fn as_any(&self) -> &dyn Any {
        unreachable!()
    }

    fn schema(&self) -> SchemaRef {
        unreachable!()
    }

    fn table_type(&self) -> TableType {
        unreachable!()
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        _projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unreachable!()
    }
}

#[cfg(test)]
mod test {
    use crate::fuzz_sql::FuzzConfig;
    use crate::{SQLRelationGenerator, SQLTable};
    use datafusion::prelude::JoinType;
    use datafusion::{
        arrow::datatypes::DataType,
        common::{DFField, DFSchema, Result},
    };
    use std::collections::HashMap;

    #[test]
    fn test() -> Result<()> {
        let mut rng = rand::thread_rng();
        let config = FuzzConfig {
            join_types: vec![JoinType::Semi],
            max_depth: 5,
        };
        let mut gen = SQLRelationGenerator::new(&mut rng, test_tables()?, config);
        let _plan = gen.generate_relation()?;
        Ok(())
    }

    fn test_tables() -> Result<Vec<SQLTable>> {
        Ok(vec![SQLTable::new(
            "foo",
            DFSchema::new_with_metadata(
                vec![
                    DFField::new(None, "a", DataType::Int8, true),
                    DFField::new(None, "b", DataType::Int32, true),
                    DFField::new(None, "c", DataType::Utf8, true),
                ],
                HashMap::new(),
            )?,
        )])
    }
}
