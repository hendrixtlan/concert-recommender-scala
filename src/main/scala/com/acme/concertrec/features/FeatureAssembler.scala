package com.acme.concertrec.features

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object FeatureAssembler {
  def assemble(
    priceFilteredCandidates: DataFrame,
    categoryProfile: DataFrame,
    eventFeatures: DataFrame
  ): DataFrame = {
    priceFilteredCandidates
      .drop("category_affinity")
      .join(
        categoryProfile.select("user_id", "category", "category_affinity"),
        Seq("user_id", "category"),
        "left"
      )
      .join(eventFeatures, Seq("event_id"), "left")
      .withColumn("days_until_event", datediff(to_date(col("event_start_ts")), current_date()))
      .withColumn("market_match", when(col("market") === col("last_market"), 1.0).otherwise(0.0))
      .withColumn("als_score", coalesce(col("als_score"), lit(0.0)))
      .withColumn("neighbor_probability", coalesce(col("neighbor_probability"), lit(0.0)))
      .withColumn("category_affinity", coalesce(col("category_affinity"), lit(0.0)))
  }
}
