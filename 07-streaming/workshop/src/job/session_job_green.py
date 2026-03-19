from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_events_source_kafka(t_env):
    table_name = "events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INTEGER,
            -- Мы можем убрать лишние поля, если они не нужны для сессий
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
         ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green_trips',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true' -- ДОБАВЬ ЭТУ СТРОКУ
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def create_session_sink_postgres(t_env):
    table_name = 'processed_events_sessions'
    # Используем window_start и window_end для определения границ сессии
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,
            PRIMARY KEY (window_start, window_end, PULocationID) NOT ENFORCED
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

def log_session_aggregation():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1) # Обязательно 1 для корректной работы Watermark с 1 партицией

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source_table = create_events_source_kafka(t_env)
        sink_table = create_session_sink_postgres(t_env)

        # Используем функцию SESSION с интервалом 5 минут
        t_env.execute_sql(f"""
        INSERT INTO {sink_table}
        SELECT
            window_start,
            window_end,
            PULocationID,
            COUNT(*) AS num_trips
        FROM TABLE(
            SESSION(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTES)
        )
        GROUP BY window_start, window_end, PULocationID;
        """).wait()

    except Exception as e:
        print("Flink Session Job failed:", str(e))

if __name__ == '__main__':
    log_session_aggregation()