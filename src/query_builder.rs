use sea_query::{BinOper, EscapeBuilder, QueryBuilder, QuotedBuilder, SelectDistinct, SqlWriter, SubQueryStatement, TableRefBuilder, Value};
use sea_query::extension::postgres::PgBinOper;
use sea_query::Write;

pub struct ClickHouseQueryBuilder;

impl QuotedBuilder for ClickHouseQueryBuilder {
    fn quote(&self) -> char {
        '"'
    }
}

impl EscapeBuilder for ClickHouseQueryBuilder {}

impl TableRefBuilder for ClickHouseQueryBuilder {}

impl QueryBuilder for ClickHouseQueryBuilder {
    fn placeholder(&self) -> (&str, bool) {
        ("$", true)
    }

    fn prepare_select_distinct(&self, select_distinct: &SelectDistinct, sql: &mut dyn SqlWriter) {
        match select_distinct {
            SelectDistinct::All => write!(sql, "ALL").unwrap(),
            SelectDistinct::Distinct => write!(sql, "DISTINCT").unwrap(),
            SelectDistinct::DistinctOn(cols) => {
                write!(sql, "DISTINCT ON (").unwrap();
                cols.iter().fold(true, |first, column_ref| {
                    if !first {
                        write!(sql, ", ").unwrap();
                    }
                    self.prepare_column_ref(column_ref, sql);
                    false
                });
                write!(sql, ")").unwrap();
            }
            _ => {}
        };
    }

    fn prepare_bin_oper(&self, bin_oper: &BinOper, sql: &mut dyn SqlWriter) {
        match bin_oper {
            BinOper::PgOperator(oper) => write!(
                sql,
                "{}",
                match oper {
                    PgBinOper::ILike => "ILIKE",
                    PgBinOper::NotILike => "NOT ILIKE",
                    PgBinOper::Matches => "@@",
                    PgBinOper::Contains => "@>",
                    PgBinOper::Contained => "<@",
                    PgBinOper::Concatenate => "||",
                    PgBinOper::Similarity => "%",
                    PgBinOper::WordSimilarity => "<%",
                    PgBinOper::StrictWordSimilarity => "<<%",
                    PgBinOper::SimilarityDistance => "<->",
                    PgBinOper::WordSimilarityDistance => "<<->",
                    PgBinOper::StrictWordSimilarityDistance => "<<<->",
                }
            )
                .unwrap(),
            _ => self.prepare_bin_oper_common(bin_oper, sql),
        }
    }

    fn prepare_query_statement(&self, _: &SubQueryStatement, _: &mut dyn SqlWriter) {}

    // fn prepare_order_expr(&self, order_expr: &OrderExpr, sql: &mut dyn SqlWriter) {
    //     if !matches!(order_expr.order, Order::Field(_)) {
    //         self.prepare_simple_expr(&order_expr.expr, sql);
    //         write!(sql, " ").unwrap();
    //     }
    //     self.prepare_order(order_expr, sql);
    //     match order_expr.nulls {
    //         None => (),
    //         Some(NullOrdering::Last) => write!(sql, " NULLS LAST").unwrap(),
    //         Some(NullOrdering::First) => write!(sql, " NULLS FIRST").unwrap(),
    //     }
    // }

    fn prepare_value(&self, value: &Value, sql: &mut dyn SqlWriter) {
        sql.push_param(value.clone(), self as _);
    }

    fn write_string_quoted(&self, string: &str, buffer: &mut String) {
        let escaped = self.escape_string(string);
        let string = if escaped.find('\\').is_some() {
            "E'".to_owned() + &escaped + "'"
        } else {
            "'".to_owned() + &escaped + "'"
        };
        write!(buffer, "{}", string).unwrap()
    }

    fn if_null_function(&self) -> &str {
        "COALESCE"
    }
}