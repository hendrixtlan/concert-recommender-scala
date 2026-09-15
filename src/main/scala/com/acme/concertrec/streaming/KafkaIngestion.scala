package com.acme.concertrec.streaming

import com.acme.concertrec.config.KafkaConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object KafkaIngestion {
  def rawStream(spark: SparkSession, cfg: KafkaConfig): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", cfg.bootstrapServers)
      .option("subscribePattern", cfg.subscribePattern)
      .option("startingOffsets", cfg.startingOffsets)
      .option("failOnDataLoss", cfg.failOnDataLoss.toString)
      .load()
      .select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("key").cast("string").alias("message_key"),
        col("value").cast("string").alias("message_value")
      )
  }
}
