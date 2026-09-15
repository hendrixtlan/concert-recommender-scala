package com.acme.concertrec.features

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object PriceFeatureEngine {
  private def snapshotAtOffset(
    history: DataFrame,
    latest: DataFrame,
    days: Int,
    outputCol: String
  ): DataFrame = {
    val joined = history.alias("h")
      .join(latest.select("event_id", "latest_ts").alias("l"), Seq("event_id"))
      .filter(col("h.observed_ts") <= expr(s"l.latest_ts - INTERVAL $days DAYS"))

    val w = Window.partitionBy(col("event_id")).orderBy(col("h.observed_ts").desc)

    joined
      .withColumn("rn", row_number().over(w))
      .filter(col("rn") === 1)
      .select(col("event_id"), col("h.price").alias(outputCol))
  }

  def eventPriceFeatures(priceHistory: DataFrame): DataFrame = {
    val wLatest = Window.partitionBy("event_id").orderBy(col("observed_ts").desc)

    val latest = priceHistory
      .withColumn("rn", row_number().over(wLatest))
      .filter(col("rn") === 1)
      .select(
        col("event_id"),
        col("price").alias("observed_current_price"),
        col("observed_ts").alias("latest_ts")
      )

    val p1d = snapshotAtOffset(priceHistory, latest, 1, "price_1d_ago")
    val p7d = snapshotAtOffset(priceHistory, latest, 7, "price_7d_ago")

    val recentVolatility = priceHistory.alias("h")
      .join(latest.select("event_id", "latest_ts").alias("l"), Seq("event_id"))
      .filter(col("h.observed_ts") >= expr("l.latest_ts - INTERVAL 7 DAYS"))
      .groupBy("event_id")
      .agg(stddev_pop(col("h.price")).alias("price_volatility_7d"))

    latest
      .join(p1d, Seq("event_id"), "left")
      .join(p7d, Seq("event_id"), "left")
      .join(recentVolatility, Seq("event_id"), "left")
      .withColumn(
        "price_change_1d",
        when(col("price_1d_ago") > 0.0, col("observed_current_price") / col("price_1d_ago") - 1.0)
      )
      .withColumn(
        "price_change_7d",
        when(col("price_7d_ago") > 0.0, col("observed_current_price") / col("price_7d_ago") - 1.0)
      )
  }

  /** Hard budget rule: current_price must not exceed lifetime_avg_price. */
  def attachUserFeatures(
    candidates: DataFrame,
    userProfile: DataFrame,
    eventPrice: DataFrame
  ): DataFrame = {
    candidates
      .join(userProfile, Seq("user_id"), "left")
      .join(eventPrice, Seq("event_id"), "left")
      .withColumn("current_price", coalesce(col("observed_current_price"), col("catalog_current_price")))
      .filter(
        col("lifetime_avg_price").isNotNull &&
        col("current_price").isNotNull &&
        col("current_price") <= col("lifetime_avg_price")
      )
      .withColumn("price_ratio", col("current_price") / col("lifetime_avg_price"))
      .withColumn(
        "price_distance",
        (col("lifetime_avg_price") - col("current_price")) / col("lifetime_avg_price")
      )
      .withColumn("value_opportunity", greatest(-coalesce(col("price_change_7d"), lit(0.0)), lit(0.0)))
      .withColumn("demand_pressure", greatest(coalesce(col("price_change_7d"), lit(0.0)), lit(0.0)))
  }
}
