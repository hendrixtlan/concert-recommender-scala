package com.acme.concertrec.jobs

import com.acme.concertrec.candidate.CandidateGenerator
import com.acme.concertrec.config.AppConfig
import com.acme.concertrec.features.{FeatureAssembler, HistoricalInteractionBuilder, PriceFeatureEngine}
import com.acme.concertrec.ml.ALSModelTrainer
import com.acme.concertrec.ranking.{ConcertRanker, DiversityReranker, HeuristicRanker}
import com.acme.concertrec.similarity.SimilarUserEngine
import com.acme.concertrec.streaming.RecommendationPublisher
import com.acme.concertrec.util.SparkSessionFactory
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object RecommendationJob {
  private def latestCatalog(catalog: DataFrame): DataFrame = {
    val w = Window.partitionBy("event_id").orderBy(col("updated_ts").desc)
    catalog
      .withColumn("rn", row_number().over(w))
      .filter(col("rn") === 1)
      .drop("rn")
  }

  def main(args: Array[String]): Unit = {
    val cfg = AppConfig.load()
    implicit val spark = SparkSessionFactory.getOrCreate(s"${cfg.appName}-recommend", cfg.timezone)

    val rawInteractions = spark.table(cfg.tables.walletInteractions)
    val weighted = HistoricalInteractionBuilder.build(rawInteractions, cfg.recommendation.halfLifeDays)
    val catalog = latestCatalog(spark.table(cfg.tables.eventCatalog))
    val categoryProfile = spark.table(cfg.tables.categoryProfile)
    val userProfile = spark.table(cfg.tables.userProfile)
    val eventFeatures = spark.table(cfg.tables.eventFeatures)
    val priceHistory = spark.table(cfg.tables.priceHistory)

    val pipeline = PipelineModel.load(cfg.paths.alsModel)
    val artifacts = ALSModelTrainer.fromPipeline(pipeline)

    val alsCandidates = ALSModelTrainer.recommendForAllUsers(
      artifacts,
      cfg.recommendation.alsCandidates
    )

    val neighbors = SimilarUserEngine.neighbors(
      artifacts,
      topK = cfg.recommendation.similarUsers,
      distanceThreshold = cfg.recommendation.similarityDistanceThreshold
    )

    val neighborCandidates = SimilarUserEngine.eventCandidatesFromNeighbors(
      neighbors,
      rawInteractions,
      cfg.recommendation.neighborCandidates
    )

    val categoryCandidates = CandidateGenerator.categoryCandidates(
      categoryProfile,
      catalog,
      cfg.recommendation.categoryCandidates
    )

    val candidates = CandidateGenerator.combine(
      alsCandidates,
      neighborCandidates,
      categoryCandidates,
      catalog,
      weighted
    )

    val eventPrice = PriceFeatureEngine.eventPriceFeatures(priceHistory)
    val budgetEligible = PriceFeatureEngine.attachUserFeatures(candidates, userProfile, eventPrice)
    val features = FeatureAssembler.assemble(budgetEligible, categoryProfile, eventFeatures)

    val scored = cfg.ranker.mode.toLowerCase match {
      case "gbt" => ConcertRanker.score(PipelineModel.load(cfg.paths.rankerModel), features)
      case _ => HeuristicRanker.score(features)
    }

    val reranked = DiversityReranker.rerank(
      scored,
      cfg.recommendation.finalTopK,
      cfg.recommendation.maxPerArtist,
      cfg.recommendation.maxPerCategory
    )
      .withColumn("recommendation_id", expr("uuid()"))
      .withColumn("generated_at", current_timestamp())
      .withColumn("impression_ts", col("generated_at"))

    // Persist scoring-time features. These rows become the supervised ranker
    // training source after the feedback window matures.
    reranked
      .write
      .format("delta")
      .mode("append")
      .saveAsTable(cfg.tables.impressions)

    val recommendations = reranked.select(
      "recommendation_id", "user_id", "event_id", "rank", "recommendation_score",
      "artist", "category", "market", "event_start_ts", "current_price",
      "lifetime_avg_price", "price_change_1d", "price_change_7d",
      "als_score", "neighbor_probability", "category_affinity", "generated_at"
    )

    recommendations
      .write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(cfg.tables.recommendations)

    if (cfg.output.publishToKafka) {
      RecommendationPublisher.publishBatch(
        recommendations,
        cfg.kafka.bootstrapServers,
        cfg.output.kafkaTopic
      )
    }
  }
}
