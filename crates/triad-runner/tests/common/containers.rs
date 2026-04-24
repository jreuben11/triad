use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::{kafka::Kafka, postgres::Postgres, redis::Redis};

pub struct TestStack {
    pub pg_url: String,
    pub kafka_brokers: String,
    pub redis_url: String,
    _pg: ContainerAsync<Postgres>,
    _kafka: ContainerAsync<Kafka>,
    _redis: ContainerAsync<Redis>,
}

impl TestStack {
    pub async fn start() -> Self {
        let pg = Postgres::default()
            .with_user("triad")
            .with_password("secret")
            .with_db_name("triad_test")
            .start()
            .await
            .expect("failed to start postgres container");

        let kafka = Kafka::default()
            .start()
            .await
            .expect("failed to start kafka container");

        let redis = Redis::default()
            .start()
            .await
            .expect("failed to start redis container");

        let pg_port = pg
            .get_host_port_ipv4(5432)
            .await
            .expect("failed to get postgres port");
        let kafka_port = kafka
            .get_host_port_ipv4(9093)
            .await
            .expect("failed to get kafka port");
        let redis_port = redis
            .get_host_port_ipv4(6379)
            .await
            .expect("failed to get redis port");

        let pg_url = format!("postgresql://triad:secret@127.0.0.1:{pg_port}/triad_test");
        let kafka_brokers = format!("127.0.0.1:{kafka_port}");
        let redis_url = format!("redis://127.0.0.1:{redis_port}");

        let pool = sqlx::PgPool::connect(&pg_url)
            .await
            .expect("failed to connect to postgres for migrations");
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("migrations failed");

        Self {
            pg_url,
            kafka_brokers,
            redis_url,
            _pg: pg,
            _kafka: kafka,
            _redis: redis,
        }
    }
}

pub struct PgStack {
    pub pg_url: String,
    _pg: ContainerAsync<Postgres>,
}

impl PgStack {
    pub async fn start() -> Self {
        let pg = Postgres::default()
            .with_user("triad")
            .with_password("secret")
            .with_db_name("triad_test")
            .start()
            .await
            .expect("failed to start postgres container");

        let pg_port = pg
            .get_host_port_ipv4(5432)
            .await
            .expect("failed to get postgres port");
        let pg_url = format!("postgresql://triad:secret@127.0.0.1:{pg_port}/triad_test");

        let pool = sqlx::PgPool::connect(&pg_url)
            .await
            .expect("failed to connect for migrations");
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("migrations failed");

        Self { pg_url, _pg: pg }
    }
}
