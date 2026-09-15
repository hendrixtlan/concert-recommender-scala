package com.acme.concertrec.ranking

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object RankerTrainingBuilder {
  /**
    * Creates mature supervised labels from recommendation impressions.
    * An impression must first mature for maturityDays. Positive feedback is
    * accepted inside labelWindowDays; true negatives are emitted only after the
    * complete label window has closed.
    */
  def fromFeedback(
    candidateFeaturesAtImpression: DataFrame,
    feedback: DataFrame,
    maturityDays: Int,
    labelWindowDays: Int
  ): DataFrame = {
    val sourceTs =
      if (candidateFeaturesAtImpression.columns.contains("impression_ts")) col("impression_ts")
      else col("generated_at")

    val impressions = candidateFeaturesAtImpression
      .withColumn("impression_ts", sourceTs)
      .filter(col("impression_ts") <= expr(s"current_timestamp() - INTERVAL $maturityDays DAYS"))
      .alias("i")

    val impressionColumns = impressions.columns
    val fb = feedback.alias("f")

    val joined = impressions
      .join(
        fb,
        col("i.recommendation_id") === col("f.recommendation_id") &&
          col("i.user_id") === col("f.user_id") &&
          col("i.event_id") === col("f.event_id") &&
          col("f.feedback_ts") >= col("i.impression_ts") &&
          col("f.feedback_ts") <= expr(s"i.impression_ts + INTERVAL $labelWindowDays DAYS"),
        "left"
      )

    joined
      .groupBy(impressionColumns.map(c => col(s"i.$c")): _*)
      .agg(
        max(
          when(
            lower(col("f.action")).isin(
              "purchased", "redeemed", "ticket_purchased", "ticket_redeemed"
            ),
            lit(1.0)
          )
            .otherwise(lit(0.0))
        ).alias("label")
      )
      .filter(
        col("label") === 1.0 ||
          col("impression_ts") <= expr(s"current_timestamp() - INTERVAL $labelWindowDays DAYS")
      )
  }
}
