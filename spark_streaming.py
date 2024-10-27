from datetime import datetime
from itertools import groupby

from pyspark.sql import SparkSession
from typing import List,Dict
from setuptools import setup
from ride import Ride
from pyspark.sql import types
from pyspark.sql.functions import from_json, col,expr,lit,avg,udf,to_date,month,sum as _sum,mean,count,window,format_number
import psycopg2

spark = (SparkSession.builder 
                    .appName('stock_streaming') 
.config("spark.jars.packages",
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")  
.config("spark.jars",
                     "/home/haianhduy/Python_Coding/RealtimeStockV2/postgresql-42.7.3.jar") 
.config('spark.sql.adaptive.enable', 'false')
                 
.getOrCreate()
)

spark.sparkContext.setLogLevel('WARN')

conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
cur = conn.cursor()

jdbc_url = "jdbc:postgresql://localhost:5432/voting"
Properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}


def sink_console(df, output_mode: str = 'complete', processing_time: str = '5 seconds'):
    write_query = df.writeStream \
        .outputMode(output_mode) \
        .trigger(processingTime=processing_time) \
        .format("console") \
        .option("truncate", False) \
        .start()
    return write_query



def proces_trend_df(open,close):
    if(open < close):
        return str('Up')
    else:
        return str('Down')

def mutation_check(valume,avg_valume):
    if (avg_valume is not None and valume >= 5 * avg_valume):
        return True
    else:
        return False

def stream_to_db_postgres(df,output_mode: str = 'complete',processing_time: str = '5 seconds',table_name:str = ''):
    write_query = (df.writeStream
                   .outputMode(output_mode)
                   .trigger(processingTime=processing_time)
                   .foreachBatch(lambda bachth_df,batch_id:bachth_df.write.jdbc(
        jdbc_url,
        table_name,
        mode="append",
        properties=Properties
    ) )
                   .start()
                   )
    return  write_query

def write_to_db(batch_df,batch_id):
    write_query = batch_df.write.jdbc(
        jdbc_url,
        "stock_1",
        mode="append",
        properties=Properties
    )
    return write_query

def create_tables(conn,cur):
    cur.execute("""
       CREATE TABLE IF NOT EXISTS stock_1 (
            time TIMESTAMP WITHOUT TIME ZONE,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume INTEGER,
            id VARCHAR(50)  PRIMARY KEY,
            test VARCHAR(50)
        )
""")
    cur.execute("""
           CREATE TABLE IF NOT EXISTS avg_by_day (
                id VARCHAR(50)  PRIMARY KEY,
                time TIMESTAMP WITHOUT TIME ZONE,
                avg_by_day FLOAT
            )
    """)

    cur.execute("""
           CREATE TABLE IF NOT EXISTS trend_by_day (
                id VARCHAR(50)  PRIMARY KEY,
                time TIMESTAMP WITHOUT TIME ZONE,
                open FLOAT,
                close FLOAT,
                trend VARCHAR(50)
            )
    """)
    cur.execute("""
           CREATE TABLE IF NOT EXISTS volume_by_month (
                month INTEGER PRIMARY KEY,
                total_by_month INTEGER
            )
    """)

    cur.execute("""
           CREATE TABLE IF NOT EXISTS mutation_check (
                window_start TIMESTAMP NOT NULL,
                window_end TIMESTAMP NOT NULL,
                total_volume BIGINT,
                avg_volume DOUBLE PRECISION,
                mutation_check BOOLEAN
            )
    """)
    conn.commit()




if __name__ == "__main__":
    create_tables(conn, cur)
    print('tao bang thanh cong !')

    schema  = types.StructType([
            types.StructField("time", types.TimestampType(), True),
            types.StructField("open", types.FloatType(), True),
            types.StructField("high", types.FloatType(), True),
            types.StructField("low", types.FloatType(), True),
            types.StructField("close", types.FloatType(), True),
            types.StructField("volume", types.IntegerType(), True),
            types.StructField("id", types.StringType(), True),
    ])

    df = (spark.readStream
        .format("kafka") 
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") 
        .option("subscribe", "stock_topic") 
        .option("startingOffsets", "earliest") 
        .load() 
        .select(expr("CAST(value AS STRING)"))
        .select(from_json( col('value'), schema).alias('data'))
        .select("data.*")
      )
    
    new_df = df.withColumn('time',col('time').cast(types.TimestampType()))
    water_mark_df = new_df.withWatermark("time","1 minute")

    # xu ly
    test_df = water_mark_df.withColumn('test',lit('test'))
    # query = sink_console(test_df,output_mode='append')

    #xu ly vag_by_day
    avg_by_day_df = water_mark_df.withColumn('avg_by_day',(col('open') + col('high') + col('low') + col('close')) / 4).select('id','time','avg_by_day')
    # query_a = sink_console(avg_by_day_df,output_mode='append')

    #xu ly trend
    proces_trend_df = udf(proces_trend_df,types.StringType())
    trend_df = water_mark_df.withColumn('trend',proces_trend_df(col('open'),col('close'))).select('id','time','open','close','trend')
    # query_b = sink_console(trend_df,output_mode='append')

    #xu ly du lieu theo tháng
    total_by_month = water_mark_df.withColumn('month',month(to_date(col('time'),'yyyy-MM-dd')))
    total_by_month_df = total_by_month.groupBy('month').agg(_sum('volume').alias('total_by_month'))

    mutation_check_udf = udf(mutation_check,types.BooleanType())
    avg_volume_df = (water_mark_df
                     .groupBy(window(col("time"), "5 days"))
                        .agg(
                            _sum(col("volume")).alias("total_volume"),
                            avg(col("volume")).alias("avg_volume")
                        )
                     )
    water_mark_df_with_window = water_mark_df.withColumn("window", window(col("time"), "5 days"))

    mutation_df = (water_mark_df_with_window
                   .join(avg_volume_df, on="window", how="inner")
                   .withColumn("mutation_check", mutation_check_udf(col("total_volume"), col("avg_volume")))

                   )
    mutation_df = mutation_df.select("window", "total_volume", "avg_volume", "mutation_check").dropDuplicates()
    result_df = mutation_df.withColumn("window_start", col("window").getField("start")) \
        .withColumn("window_end", col("window").getField("end")) \
        .select("window_start", "window_end", "total_volume", "avg_volume", "mutation_check")

    # query = sink_console(result_df,output_mode="append")
    # query.awaitTermination()


    # spark-streaming write vao database
    query_a = stream_to_db_postgres(df=avg_by_day_df,processing_time='5 seconds',output_mode='append',table_name='avg_by_day')
    query_b = stream_to_db_postgres(df=trend_df,processing_time='5 seconds',output_mode='append',table_name='trend_by_day')
    query_c = stream_to_db_postgres(df=total_by_month_df,processing_time='5 seconds',output_mode='update',table_name='volume_by_month')
    query_d = stream_to_db_postgres(df=result_df,processing_time='5 seconds',output_mode='append',table_name='mutation_check')

    query_a.awaitTermination()
    query_b.awaitTermination()
    query_c.awaitTermination()
    query_d.awaitTermination()










