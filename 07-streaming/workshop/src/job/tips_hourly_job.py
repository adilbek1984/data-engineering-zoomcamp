from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_events_source_kafka(t_env):
    table_name = "events_tips"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            tip_amount DOUBLE, -- Используем DOUBLE как ты и просил
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '15' SECOND
         ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green_trips',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def create_tips_sink_postgres(t_env):
    table_name = 'processed_tips_hourly'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            total_tips DOUBLE,
            PRIMARY KEY (window_start, window_end) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name

def log_tips_aggregation():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source_table = create_events_source_kafka(t_env)
        sink_table = create_tips_sink_postgres(t_env)

        # Используем TUMBLE (фиксированное окно) на 1 час
        t_env.execute_sql(f"""
        INSERT INTO {sink_table}
        SELECT
            window_start,
            window_end,
            SUM(tip_amount) AS total_tips
        FROM TABLE(
            TUMBLE(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '1' HOURS)
        )
        GROUP BY window_start, window_end;
        """).wait()

    except Exception as e:
        print("Flink Tips Job failed:", str(e))

if __name__ == '__main__':
    log_tips_aggregation()