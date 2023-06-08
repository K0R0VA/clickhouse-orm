mod query_builder;
mod client;
mod error;


#[cfg(test)]
mod tests {
    use sea_query::{Alias, Condition, Expr, Query};
    use serde_json::Value;
    use crate::client::ClickhouseClient;
    use crate::error::Error;
    use crate::query_builder::ClickHouseQueryBuilder;

    #[test]
    fn basic_select()  {
        let query = Query::select()
            .from(Alias::new("users"))
            .column(Alias::new("name"))
            .to_string(ClickHouseQueryBuilder);
        assert_eq!("SELECT \"name\" FROM \"users\"", query);
    }

    #[test]
    fn select_with_eq() {
        let query = Query::select()
            .from(Alias::new("users"))
            .column(Alias::new("name"))
            .cond_where(Condition::all().add(Expr::col(Alias::new("name")).eq("serega")))
            .to_string(ClickHouseQueryBuilder);
        assert_eq!("SELECT \"name\" FROM \"users\" WHERE \"name\" = 'serega'", query);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fetch_many() -> Result<(), Error> {
        dotenv::dotenv().ok();
        let client = ClickhouseClient::from_env()?;
        let sql = Query::select()
            .expr(Expr::asterisk())
            .from(Alias::new("service_cycles"))
            .limit(100)
            .to_string(ClickHouseQueryBuilder);
        let cycles: Vec<Value> = client.fetch_many(&sql).await?;
        println!("{:#?}", cycles);
        Ok(())
    }
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_one() -> Result<(), Error> {
        dotenv::dotenv().ok();
        let client = ClickhouseClient::from_env()?;
        let sql = Query::select()
            .expr(Expr::asterisk())
            .from(Alias::new("service_cycles"))
            .limit(1)
            .to_string(ClickHouseQueryBuilder);
        println!("{sql}");
        let cycles: Option<Value> = client.fetch_one(&sql).await?;
        println!("{:#?}", cycles);
        Ok(())
    }
}
