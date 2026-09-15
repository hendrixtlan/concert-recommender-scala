package com.acme.concertrec.streaming

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object RecommendationPublisher {
  def publishBatch(
    recommendations: DataFrame,
    bootstrapServers: String,
    topic: String
  ): Unit = {
    recommendations
      .select(
        col("user_id").cast("string").alias("key"),
        to_json(
          struct(
            col("recommendation_id"), col("user_id"), col("event_id"),
            col("rank"), col("recommendation_score"), col("artist"),
            col("category"), col("market"), col("event_start_ts"),
            col("current_price"), col("generated_at")
          )
        ).alias("value")
      )
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("topic", topic)
      .save()
  }
}
