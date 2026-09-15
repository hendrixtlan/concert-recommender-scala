package com.acme.concertrec.similarity

import com.acme.concertrec.ml.{ALSArtifacts, ALSModelTrainer}
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, Normalizer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SimilarUserEngine {
  private val toVector = udf((xs: Seq[Float]) => Vectors.dense(xs.map(_.toDouble).toArray))

  def neighbors(
    artifacts: ALSArtifacts,
    topK: Int,
    distanceThreshold: Double,
    bucketLength: Double = 1.0,
    hashTables: Int = 6
  )(implicit spark: SparkSession): DataFrame = {
    val userMap = ALSModelTrainer.userIndexMap(artifacts)

    val factors = artifacts.alsModel.userFactors
      .withColumnRenamed("id", "user_idx")
      .join(userMap, Seq("user_idx"))
      .withColumn("features_vec", toVector(col("features")))

    val normalizer = new Normalizer()
      .setInputCol("features_vec")
      .setOutputCol("features_unit")
      .setP(2.0)

    val normalized = normalizer.transform(factors)
      .select("user_id", "features_unit")

    val lsh = new BucketedRandomProjectionLSH()
      .setInputCol("features_unit")
      .setOutputCol("hashes")
      .setBucketLength(bucketLength)
      .setNumHashTables(hashTables)

    val lshModel = lsh.fit(normalized)

    val joined = lshModel.approxSimilarityJoin(
      normalized,
      normalized,
      distanceThreshold,
      "euclidean_distance"
    )

    val candidates = joined
      .select(
        col("datasetA.user_id").alias("user_id"),
        col("datasetB.user_id").alias("neighbor_user_id"),
        col("euclidean_distance")
      )
      .filter(col("user_id") =!= col("neighbor_user_id"))
      .withColumn(
        "cosine_similarity",
        greatest(
          lit(-1.0),
          least(lit(1.0), lit(1.0) - pow(col("euclidean_distance"), 2.0) / lit(2.0))
        )
      )
      .filter(col("cosine_similarity") > 0.0)

    val w = Window.partitionBy("user_id").orderBy(col("cosine_similarity").desc)
    candidates
      .withColumn("rn", row_number().over(w))
      .filter(col("rn") <= topK)
      .drop("rn", "euclidean_distance")
  }

  def eventCandidatesFromNeighbors(
    neighbors: DataFrame,
    attendedEvents: DataFrame,
    topN: Int
  ): DataFrame = {
    val attended = attendedEvents
      .filter(col("event_type") === "ticket_redeemed")
      .select("user_id", "event_id")
      .distinct()
      .withColumnRenamed("user_id", "neighbor_user_id")

    val denominator = neighbors
      .groupBy("user_id")
      .agg(sum("cosine_similarity").alias("neighbor_similarity_total"))

    val weighted = neighbors
      .join(attended, Seq("neighbor_user_id"))
      .groupBy("user_id", "event_id")
      .agg(sum("cosine_similarity").alias("neighbor_event_support"))
      .join(denominator, Seq("user_id"))
      .withColumn(
        "neighbor_probability",
        when(col("neighbor_similarity_total") > 0.0,
          col("neighbor_event_support") / col("neighbor_similarity_total")
        ).otherwise(lit(0.0))
      )

    val w = Window.partitionBy("user_id").orderBy(col("neighbor_probability").desc)
    weighted
      .withColumn("rn", row_number().over(w))
      .filter(col("rn") <= topN)
      .drop("rn", "neighbor_event_support", "neighbor_similarity_total")
  }
}
