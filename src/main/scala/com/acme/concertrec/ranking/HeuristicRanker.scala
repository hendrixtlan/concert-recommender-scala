package com.acme.concertrec.ranking

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object HeuristicRanker {
  /**
    * Bootstrap ranker used before enough recommendation exposure/feedback exists
    * to train the supervised GBT ranker.
    */
  def score(features: DataFrame): DataFrame = {
    val als = coalesce(col("als_score"), lit(0.0))
    val neighbors = coalesce(col("neighbor_probability"), lit(0.0))
    val category = coalesce(col("category_affinity"), lit(0.0))
    val value = least(greatest(coalesce(col("price_distance"), lit(0.0)), lit(0.0)), lit(1.0))
    val growth = least(greatest(coalesce(col("popularity_growth"), lit(0.0)), lit(-1.0)), lit(1.0))
    val market = coalesce(col("market_match"), lit(0.0))

    // ALS scores are not calibrated probabilities; squash them to [0,1].
    val alsSquashed = lit(1.0) / (lit(1.0) + exp(-als))

    features.withColumn(
      "recommendation_score",
      lit(0.33) * neighbors +
        lit(0.24) * category +
        lit(0.19) * alsSquashed +
        lit(0.09) * value +
        lit(0.08) * ((growth + lit(1.0)) / lit(2.0)) +
        lit(0.07) * market
    )
  }
}
