from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, lit, explode, udf, arrays_zip, collect_list, to_date, unix_timestamp
from pyspark.sql.types import StringType, ArrayType

spark = SparkSession.builder.getOrCreate()
sqlContext = SQLContext(spark)


def build_session_id(user_id, session_id):
    return f"{user_id}_{session_id}"


def calculate_sessions(user_id, diff):
    session_ids = []
    session_count = 0
    total_session_time = 0

    for diff in diff:
        total_session_time = total_session_time + diff

        if diff > 60 * 30 or total_session_time > 60 * 60 * 2:
            if total_session_time > 60 * 60 * 2:
                total_session_time = 0
            session_count = session_count + 1

        session_ids.append(build_session_id(user_id, session_count))

    return session_ids


calculate_sessions_udf = udf(calculate_sessions, ArrayType(StringType()))


# Part A
def sessionise(df):
    df = df.select(col("User_id"), col("Timestamp"), col(
        "Timestamp").alias('ts').cast("timestamp"))

    # Create a window over the timestamp
    w = Window.partitionBy("User_id").orderBy("ts")
    df = df.withColumn("lag_timestamp", lag("ts", 1).over(w))

    # Calculate the difference in seconds between clicks
    df = df.withColumn(
        "diff", ((unix_timestamp("ts") - unix_timestamp("lag_timestamp"))))
    df = df.na.fill(0)

    # Group the clicks by user id
    df = df.groupBy("User_id").agg(collect_list("Timestamp").alias(
        "Timestamp"), collect_list("diff").alias("diff"))

    # Calculate the sessions
    df = df.withColumn("session_id", calculate_sessions_udf(lit(col("User_id")), col("diff"))) \
        .select("User_id", "session_id", "Timestamp")

    # Zip the arrays back together, select the fields we need
    df = df.withColumn("session_data", arrays_zip("session_id", "Timestamp")) \
        .withColumn("session_data", explode(col("session_data"))) \
        .select("User_id", col("session_data.Timestamp").alias("Timestamp"), col("session_data.session_id").alias("Session_id"))

    return df


def main():

    # Read the data
    df = spark.read.csv("./data.csv", header=True, sep=",")

    # Part A
    df = sessionise(df)

    # Output Part A
    df.repartition(1).write.format("parquet").mode(
        "overwrite").save("sessions.parquet")

    # Part B
    df = df.select("User_Id", "Timestamp", "Session_id",
                   to_date("Timestamp").alias('Date'))

    # Create date paritions
    df.write.partitionBy("Date").bucketBy(4, "User_id") \
        .saveAsTable("session_data", "parquet", "overwrite")

    # Get Number of sessions generated for each day
    sqlContext.sql("""
        SELECT Date, COUNT(*) as count
        FROM session_data
        GROUP BY Date
    """).show()

    # Total time spent by a user in a day
    # NOTE: Assuming that sessions with a single click don't count
    sqlContext.sql("""
        SELECT Date, User_id, SUM(time_spent) as Total_time_spent
        FROM (
            SELECT
                Date,
                User_id,
                Session_id,
                (
                    MAX(to_unix_timestamp(CAST(Timestamp as timestamp))) - 
                    MIN(to_unix_timestamp(CAST(Timestamp as timestamp)))
                ) as time_spent
            FROM session_data
            GROUP BY Date, User_id, Session_id
        ) GROUP BY Date, User_id
    """).show()


if __name__ == "__main__":
    main()
