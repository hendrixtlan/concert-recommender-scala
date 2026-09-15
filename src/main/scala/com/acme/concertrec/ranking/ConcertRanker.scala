package com.acme.concertrec.ranking

import com.acme.concertrec.config.RankerConfig
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.feature.{Imputer, VectorAssembler}
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ConcertRanker {
  val featureColumns: Array[String] = Array(
    "als_score",
    "neighbor_probability",
    "category_affinity",
    "price_ratio",
    "price_distance",
    "price_change_1d",
    "price_change_7d",
    "price_volatility_7d",
    "value_opportunity",
    "demand_pressure",
    "redeemed_30d",
    "purchases_7d",
    "popularity_growth",
    "historical_event_count",
    "historical_strength",
    "category_diversity",
    "days_since_last_event",
    "days_until_event",
    "market_match"
  )

  def train(training: DataFrame, cfg: RankerConfig): PipelineModel = {
    val imputedCols = featureColumns.map(_ + "_imputed")

    val imputer = new Imputer()
      .setInputCols(featureColumns)
      .setOutputCols(imputedCols)
      .setStrategy("median")

    val assembler = new VectorAssembler()
      .setInputCols(imputedCols)
      .setOutputCol("features")

    val gbt = new GBTClassifier()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setMaxIter(cfg.maxIter)
      .setMaxDepth(cfg.maxDepth)
      .setStepSize(cfg.stepSize)
      .setSeed(42L)

    new Pipeline().setStages(Array(imputer, assembler, gbt)).fit(training)
  }

  def score(model: PipelineModel, features: DataFrame): DataFrame = {
    val scored = model.transform(features)
      .withColumn("recommendation_score", vector_to_array(col("probability")).getItem(1))

    val transient = featureColumns.map(_ + "_imputed") ++
      Array("features", "rawPrediction", "probability", "prediction")

    scored.drop(transient: _*)
  }
}
