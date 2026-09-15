package com.acme.concertrec.candidate

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object CandidateGenerator {
  def categoryCandidates(
    categoryProfile: DataFrame,
    eventCatalog: DataFrame,
    topN: Int
  ): DataFrame = {
    val future = eventCatalog
      .filter(col("event_start_ts") > current_timestamp())
      .filter(lower(coalesce(col("status"), lit("active"))).isin("active", "onsale", "on_sale"))

    val joined = categoryProfile
      .join(
        future.select("event_id", "category", "artist", "market", "event_start_ts"),
        Seq("category")
      )
      .select("user_id", "event_id", "category_affinity")

    val w = Window.partitionBy("user_id").orderBy(col("category_affinity").desc)
    joined
      .withColumn("rn", row_number().over(w))
      .filter(col("rn") <= topN)
      .drop("rn")
  }

  def combine(
    alsCandidates: DataFrame,
    neighborCandidates: DataFrame,
    categoryCandidates: DataFrame,
    eventCatalog: DataFrame,
    knownHistory: DataFrame
  ): DataFrame = {
    val als = alsCandidates.select(
      col("user_id"), col("event_id"),
      col("als_score"),
      lit(null).cast("double").alias("neighbor_probability"),
      lit(null).cast("double").alias("category_affinity")
    )

    val neighbors = neighborCandidates.select(
      col("user_id"), col("event_id"),
      lit(null).cast("double").alias("als_score"),
      col("neighbor_probability"),
      lit(null).cast("double").alias("category_affinity")
    )

    val category = categoryCandidates.select(
      col("user_id"), col("event_id"),
      lit(null).cast("double").alias("als_score"),
      lit(null).cast("double").alias("neighbor_probability"),
      col("category_affinity")
    )

    val alreadyKnown = knownHistory
      .filter(col("interaction_strength") > 0.0)
      .select("user_id", "event_id")
      .distinct()
      .withColumn("already_known", lit(1))

    als.unionByName(neighbors)
      .unionByName(category)
      .groupBy("user_id", "event_id")
      .agg(
        max("als_score").alias("als_score"),
        max("neighbor_probability").alias("neighbor_probability"),
        max("category_affinity").alias("category_affinity")
      )
      .join(alreadyKnown, Seq("user_id", "event_id"), "left")
      .filter(col("already_known").isNull)
      .drop("already_known")
      .join(
        eventCatalog
          .filter(col("event_start_ts") > current_timestamp())
          .withColumnRenamed("current_price", "catalog_current_price")
          .select(
            "event_id", "category", "subcategory", "artist", "venue_id",
            "market", "event_start_ts", "catalog_current_price", "status"
          ),
        Seq("event_id")
      )
  }
}
