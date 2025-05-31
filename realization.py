import os

from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType


TOPIC_IN = "daria-kolbasina_in"
TOPIC_OUT = "daria-kolbasina_out"


# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()
    # записываем df в PostgreSQL с полем feedback
    df_with_feedback = df.withColumn("feedback", F.lit(""))
    df_with_feedback.write.format("jdbc").option(
        "url", "jdbc:postgresql://localhost:5432/de"
    ).option("dbtable", "public.subscribers_feedback").option("user", "jovyan").option(
        "password", "jovyan"
    ).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "append"
    ).save()
    # создаём df для отправки в Kafka. Сериализация в json.
    kafka_df = df.withColumn(
        "value",
        F.to_json(
            F.struct(
                F.col("restaurant_id"),
                F.col("adv_campaign_id"),
                F.col("adv_campaign_content"),
                F.col("adv_campaign_owner"),
                F.col("adv_campaign_owner_contact"),
                F.col("adv_campaign_datetime_start"),
                F.col("adv_campaign_datetime_end"),
                F.col("client_id"),
                F.col("datetime_created"),
                F.col("trigger_datetime_created"),
            )
        ),
    ).selectExpr("CAST(NULL AS STRING) AS key", "value")
    # отправляем сообщения в результирующий топик Kafka без поля feedback
    kafka_df.write.format("kafka").option(
        "kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091"
    ).option("topic", TOPIC_OUT).save()
    # очищаем память от df
    df.unpersist()


# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.postgresql:postgresql:42.4.0",
    ]
)

# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = (
    SparkSession.builder.appName("RestaurantSubscribeStreamingService")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.jars.packages", spark_jars_packages)
    .getOrCreate()
)

# читаем из топика Kafka сообщения с акциями от ресторанов
restaurant_read_stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091")
    .option("kafka.security.protocol", "SASL_SSL")
    .option(
        "kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.scram.ScramLoginModule required username="login" password="password";',
    )
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
    .option("subscribe", TOPIC_IN)
    .load()
)

# определяем схему входного сообщения для json
incomming_message_schema = StructType(
    [
        StructField("restaurant_id", StringType(), nullable=False),
        StructField("adv_campaign_id", StringType(), nullable=False),
        StructField("adv_campaign_content", StringType(), nullable=True),
        StructField("adv_campaign_owner", StringType(), nullable=True),
        StructField("adv_campaign_owner_contact", StringType(), nullable=True),
        StructField("adv_campaign_datetime_start", LongType(), nullable=False),
        StructField("adv_campaign_datetime_end", LongType(), nullable=False),
        StructField("datetime_created", LongType(), nullable=True),
    ]
)

# определяем текущее время в UTC в миллисекундах, затем округляем до секунд
current_timestamp_utc = int(round(F.unix_timestamp(F.current_timestamp())))

# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091")
    .option("subscribe", TOPIC_IN)
    .load()
    .selectExpr("CAST(value AS STRING) as json_value")
    .select(F.from_json("json_value", incomming_message_schema).alias("data"))
    .select("data.*")
    .filter(
        (current_timestamp_utc >= F.col("adv_campaign_datetime_start"))
        & (current_timestamp_utc <= F.col("adv_campaign_datetime_end"))
    )
)

# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = (
    spark.read.format("jdbc")
    .option(
        "url", "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de"
    )
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", "subscribers_restaurants")
    .option("user", "student")
    .option("password", "de-student")
    .load()
)

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
result_df = (
    filtered_read_stream_df.join(
        subscribers_restaurant_df,
        "restaurant_id",
        "inner",
    )
    .withColumn("trigger_datetime_created", F.lit(current_timestamp_utc).cast("long"))
    .select(
        "restaurant_id",
        "adv_campaign_id",
        "adv_campaign_content",
        "adv_campaign_owner",
        "adv_campaign_owner_contact",
        "adv_campaign_datetime_start",
        "adv_campaign_datetime_end",
        "client_id",
        "datetime_created",
        "trigger_datetime_created",
    )
)
# запускаем стриминг
result_df.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()
