package com.acme.concertrec.features

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object UserProfileBuilder {
  def budgetProfile(interactions: DataFrame): DataFrame = {
    val purchases = interactions
      .filter(col("event_type") === "ticket_purchased")
      .filter(col("ticket_price").isNotNull && col("ticket_price") > 0.0)
      .dropDuplicates("user_id", "ticket_id", "event_id", "event_ts")

    val lifetime = purchases
      .groupBy("user_id")
      .agg(
        avg("ticket_price").alias("lifetime_avg_price"),
        expr("percentile_approx(ticket_price, 0.5)").alias("median_price"),
        expr("percentile_approx(ticket_price, 0.75)").alias("p75_price"),
        max("ticket_price").alias("max_price"),
        stddev_pop("ticket_price").alias("price_stddev"),
        count(lit(1)).alias("tickets_purchased")
      )

    val recent = purchases
      .filter(col("event_ts") >= expr("current_timestamp() - INTERVAL 180 DAYS"))
      .groupBy("user_id")
      .agg(avg("ticket_price").alias("recent_avg_price"))

    lifetime.join(recent, Seq("user_id"), "left")
  }

  def categoryProfile(weightedHistory: DataFrame): DataFrame = {
    val byCategory = weightedHistory
      .filter(col("category").isNotNull && col("interaction_strength") > 0.0)
      .groupBy("user_id", "category")
      .agg(sum("interaction_strength").alias("category_strength"))

    val totals = byCategory
      .groupBy("user_id")
      .agg(sum("category_strength").alias("total_category_strength"))

    byCategory
      .join(totals, Seq("user_id"))
      .withColumn(
        "category_affinity",
        when(col("total_category_strength") > 0.0,
          col("category_strength") / col("total_category_strength")
        ).otherwise(lit(0.0))
      )
      .drop("total_category_strength")
  }

  def behavioralProfile(weightedHistory: DataFrame): DataFrame = {
    val positive = weightedHistory.filter(col("interaction_strength") > 0.0)

    positive
      .groupBy("user_id")
      .agg(
        countDistinct("event_id").alias("historical_event_count"),
        sum("interaction_strength").alias("historical_strength"),
        max("event_ts").alias("last_event_ts"),
        countDistinct("category").alias("category_diversity"),
        max_by(col("market"), col("event_ts")).alias("last_market")
      )
      .withColumn("days_since_last_event", datediff(current_date(), to_date(col("last_event_ts"))))
  }

  def combined(weightedHistory: DataFrame, rawInteractions: DataFrame): DataFrame = {
    behavioralProfile(weightedHistory)
      .join(budgetProfile(rawInteractions), Seq("user_id"), "left")
  }

  def topCategories(categoryProfile: DataFrame, n: Int): DataFrame = {
    val w = Window.partitionBy("user_id").orderBy(col("category_affinity").desc)
    categoryProfile
      .withColumn("rn", row_number().over(w))
      .filter(col("rn") <= n)
      .drop("rn")
  }
}
