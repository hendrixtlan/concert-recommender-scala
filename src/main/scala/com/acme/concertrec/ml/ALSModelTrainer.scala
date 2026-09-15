package com.acme.concertrec.ml

import com.acme.concertrec.config.ALSConfig
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{SQLTransformer, StringIndexer, StringIndexerModel}
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

final case class ALSArtifacts(
  pipelineModel: PipelineModel,
  userIndexer: StringIndexerModel,
  eventIndexer: StringIndexerModel,
  alsModel: ALSModel
)

object ALSModelTrainer {
  def train(training: DataFrame, cfg: ALSConfig): ALSArtifacts = {
    val userIndexer = new StringIndexer()
      .setInputCol("user_id")
      .setOutputCol("user_idx_raw")
      .setHandleInvalid("skip")

    val eventIndexer = new StringIndexer()
      .setInputCol("event_id")
      .setOutputCol("event_idx_raw")
      .setHandleInvalid("skip")

    val castIds = new SQLTransformer().setStatement(
      "SELECT *, CAST(user_idx_raw AS INT) AS user_idx, CAST(event_idx_raw AS INT) AS event_idx FROM __THIS__"
    )

    val als = new ALS()
      .setUserCol("user_idx")
      .setItemCol("event_idx")
      .setRatingCol("interaction_strength")
      .setImplicitPrefs(true)
      .setRank(cfg.rank)
      .setMaxIter(cfg.maxIter)
      .setRegParam(cfg.regParam)
      .setAlpha(cfg.alpha)
      .setColdStartStrategy("drop")
      .setNonnegative(true)

    val pipeline = new Pipeline().setStages(Array(userIndexer, eventIndexer, castIds, als))
    val model = pipeline.fit(training)

    ALSArtifacts(
      model,
      model.stages(0).asInstanceOf[StringIndexerModel],
      model.stages(1).asInstanceOf[StringIndexerModel],
      model.stages(3).asInstanceOf[ALSModel]
    )
  }

  def fromPipeline(model: PipelineModel): ALSArtifacts = {
    ALSArtifacts(
      model,
      model.stages(0).asInstanceOf[StringIndexerModel],
      model.stages(1).asInstanceOf[StringIndexerModel],
      model.stages(3).asInstanceOf[ALSModel]
    )
  }

  def userIndexMap(artifacts: ALSArtifacts)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    artifacts.userIndexer.labelsArray.head.zipWithIndex
      .map { case (userId, idx) => (idx, userId) }
      .toSeq
      .toDF("user_idx", "user_id")
  }

  def eventIndexMap(artifacts: ALSArtifacts)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    artifacts.eventIndexer.labelsArray.head.zipWithIndex
      .map { case (eventId, idx) => (idx, eventId) }
      .toSeq
      .toDF("event_idx", "event_id")
  }

  def recommendForAllUsers(artifacts: ALSArtifacts, n: Int)(implicit spark: SparkSession): DataFrame = {
    val users = userIndexMap(artifacts)
    val events = eventIndexMap(artifacts)

    artifacts.alsModel
      .recommendForAllUsers(n)
      .select(col("user_idx"), explode(col("recommendations")).alias("rec"))
      .select(
        col("user_idx"),
        col("rec.event_idx").alias("event_idx"),
        col("rec.rating").cast("double").alias("als_score")
      )
      .join(users, Seq("user_idx"))
      .join(events, Seq("event_idx"))
      .select("user_id", "event_id", "als_score")
  }
}
