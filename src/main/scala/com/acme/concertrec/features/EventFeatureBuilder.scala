package com.acme.concertrec.features

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object EventFeatureBuilder {
  def build(interactions: DataFrame): DataFrame = {
    val base = interactions
      .filter(col("event_id").isNotNull)
      .withColumn(
        "is_redeemed_30d",
        when(
          col("event_type") === "ticket_redeemed" &&
          col("event_ts") >= expr("current_timestamp() - INTERVAL 30 DAYS"), 1.0
        ).otherwise(0.0)
      )
      .withColumn(
        "is_purchase_7d",
        when(
          col("event_type") === "ticket_purchased" &&
          col("event_ts") >= expr("current_timestamp() - INTERVAL 7 DAYS"), 1.0
        ).otherwise(0.0)
      )
      .withColumn(
        "is_purchase_prev_7d",
        when(
          col("event_type") === "ticket_purchased" &&
          col("event_ts") < expr("current_timestamp() - INTERVAL 7 DAYS") &&
          col("event_ts") >= expr("current_timestamp() - INTERVAL 14 DAYS"), 1.0
        ).otherwise(0.0)
      )

    base.groupBy("event_id")
      .agg(
        sum("is_redeemed_30d").alias("redeemed_30d"),
        sum("is_purchase_7d").alias("purchases_7d"),
        sum("is_purchase_prev_7d").alias("purchases_prev_7d"),
        countDistinct(when(col("event_type") === "ticket_purchased", col("user_id")))
          .alias("distinct_buyers")
      )
      .withColumn(
        "popularity_growth",
        (col("purchases_7d") + lit(1.0)) / (col("purchases_prev_7d") + lit(1.0)) - lit(1.0)
      )
  }
}
