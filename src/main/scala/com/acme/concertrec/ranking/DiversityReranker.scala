package com.acme.concertrec.ranking

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object DiversityReranker {
  /**
    * Pragmatic production-safe diversity layer: cap repeated artists and
    * categories, then take the highest scores globally per user.
    */
  def rerank(
    scored: DataFrame,
    topK: Int,
    maxPerArtist: Int,
    maxPerCategory: Int
  ): DataFrame = {
    val artistW = Window
      .partitionBy("user_id", "artist")
      .orderBy(col("recommendation_score").desc)

    val categoryW = Window
      .partitionBy("user_id", "category")
      .orderBy(col("recommendation_score").desc)

    val overallW = Window
      .partitionBy("user_id")
      .orderBy(col("recommendation_score").desc)

    scored
      .withColumn("artist_rank", row_number().over(artistW))
      .filter(col("artist_rank") <= maxPerArtist)
      .withColumn("category_rank", row_number().over(categoryW))
      .filter(col("category_rank") <= maxPerCategory)
      .withColumn("rank", row_number().over(overallW))
      .filter(col("rank") <= topK)
      .drop("artist_rank", "category_rank")
  }
}
