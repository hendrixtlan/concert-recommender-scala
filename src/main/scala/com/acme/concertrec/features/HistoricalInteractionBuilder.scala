package com.acme.concertrec.features

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object HistoricalInteractionBuilder {
  /**
    * Full-history interaction strength with exponential half-life decay.
    * Ratings sent to implicit ALS remain non-negative. Refund/transfer signals are
    * preserved separately for downstream ranking and diagnostics.
    */
  def build(
    interactions: DataFrame,
    halfLifeDays: Double,
    asOfTsExpr: String = "current_timestamp()"
  ): DataFrame = {
    val actionWeight =
      when(col("event_type") === "ticket_redeemed", lit(4.0))
        .when(col("event_type") === "ticket_purchased", lit(2.5))
        .when(col("event_type") === "event_clicked", lit(0.5))
        .otherwise(lit(0.0))

    val negativeSignal =
      when(col("event_type") === "ticket_refunded", lit(1.0))
        .when(col("event_type") === "ticket_transferred", lit(0.5))
        .otherwise(lit(0.0))

    interactions
      .withColumn("action_weight", actionWeight)
      .withColumn("negative_signal", negativeSignal)
      .withColumn(
        "age_days",
        greatest(
          datediff(to_date(expr(asOfTsExpr)), to_date(col("event_ts"))),
          lit(0)
        )
      )
      .withColumn(
        "recency_weight",
        exp(-lit(math.log(2.0)) * col("age_days") / lit(halfLifeDays))
      )
      .withColumn(
        "interaction_strength",
        col("action_weight") * col("recency_weight")
      )
      .withColumn(
        "negative_strength",
        col("negative_signal") * col("recency_weight")
      )
  }

  def positiveForALS(weighted: DataFrame): DataFrame = {
    weighted
      .filter(col("interaction_strength") > 0.0)
      .groupBy("user_id", "event_id")
      .agg(
        sum("interaction_strength").alias("interaction_strength"),
        max("event_ts").alias("last_interaction_ts")
      )
  }
}
