mod query_builder;
mod client;
mod error;

pub use query_builder::ClickHouseQueryBuilder;
pub use client::{DeserializeError, ClickhouseClient, DatabaseError};
pub use error::Error;

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
        let _ = client.fetch_many::<Value>(&sql).await?;
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
        let _ = client.fetch_one::<Value>(&sql).await?;
        Ok(())
    }
    #[tokio::test(flavor = "current_thread")]
    async fn handle_database_error() -> Result<(), Error> {
        dotenv::dotenv().ok();
        let client = ClickhouseClient::from_env()?;
        let sql = Query::select()
            .expr(Expr::asterisk())
            .from(Alias::new("service_cycles_exception"))
            .limit(1)
            .to_string(ClickHouseQueryBuilder);
        let error = client.fetch_one::<Value>(&sql).await;
        let is_database_error =  match error {
            Err(Error::Database(_)) => true,
            _ => false
        };
        assert!(is_database_error);
        Ok(())
    }
    #[tokio::test(flavor = "current_thread")]
    async fn handle_deserialize_error() -> Result<(), Error> {
        dotenv::dotenv().ok();
        let client = ClickhouseClient::from_env()?;
        let sql = Query::select()
            .expr(Expr::asterisk())
            .from(Alias::new("service_cycles"))
            .limit(1)
            .to_string(ClickHouseQueryBuilder);
        let error = client.fetch_one::<i32>(&sql).await;
        let is_deserialize_error =  match error {
            Err(Error::DeserializeError(error)) => {
                println!("{}", error);
                true
            },
            _ => false
        };
        assert!(is_deserialize_error);
        Ok(())
    }
}
