package com.acme.concertrec.config

import com.typesafe.config.{Config, ConfigFactory}

final case class KafkaConfig(
  bootstrapServers: String,
  subscribePattern: String,
  startingOffsets: String,
  failOnDataLoss: Boolean
)

final case class RecommendationConfig(
  halfLifeDays: Double,
  alsCandidates: Int,
  similarUsers: Int,
  neighborCandidates: Int,
  categoryCandidates: Int,
  finalTopK: Int,
  similarityDistanceThreshold: Double,
  maxPerArtist: Int,
  maxPerCategory: Int
)

final case class ALSConfig(rank: Int, maxIter: Int, regParam: Double, alpha: Double)
final case class RankerConfig(
  mode: String,
  maxIter: Int,
  maxDepth: Int,
  stepSize: Double,
  maturityDays: Int,
  labelWindowDays: Int
)
final case class PathConfig(checkpointRoot: String, alsModel: String, rankerModel: String)
final case class OutputConfig(publishToKafka: Boolean, kafkaTopic: String)

final case class TableConfig(
  bronzeKafka: String,
  walletInteractions: String,
  priceHistory: String,
  eventCatalog: String,
  feedback: String,
  userProfile: String,
  categoryProfile: String,
  eventFeatures: String,
  impressions: String,
  rankerTraining: String,
  recommendations: String
)

final case class AppConfig(
  appName: String,
  timezone: String,
  kafka: KafkaConfig,
  recommendation: RecommendationConfig,
  als: ALSConfig,
  ranker: RankerConfig,
  paths: PathConfig,
  output: OutputConfig,
  tables: TableConfig
)

object AppConfig {
  def load(config: Config = ConfigFactory.load()): AppConfig = {
    AppConfig(
      appName = config.getString("app.name"),
      timezone = config.getString("app.timezone"),
      kafka = KafkaConfig(
        bootstrapServers = config.getString("kafka.bootstrapServers"),
        subscribePattern = config.getString("kafka.subscribePattern"),
        startingOffsets = config.getString("kafka.startingOffsets"),
        failOnDataLoss = config.getBoolean("kafka.failOnDataLoss")
      ),
      recommendation = RecommendationConfig(
        halfLifeDays = config.getDouble("recommendation.halfLifeDays"),
        alsCandidates = config.getInt("recommendation.alsCandidates"),
        similarUsers = config.getInt("recommendation.similarUsers"),
        neighborCandidates = config.getInt("recommendation.neighborCandidates"),
        categoryCandidates = config.getInt("recommendation.categoryCandidates"),
        finalTopK = config.getInt("recommendation.finalTopK"),
        similarityDistanceThreshold = config.getDouble("recommendation.similarityDistanceThreshold"),
        maxPerArtist = config.getInt("recommendation.maxPerArtist"),
        maxPerCategory = config.getInt("recommendation.maxPerCategory")
      ),
      als = ALSConfig(
        rank = config.getInt("als.rank"),
        maxIter = config.getInt("als.maxIter"),
        regParam = config.getDouble("als.regParam"),
        alpha = config.getDouble("als.alpha")
      ),
      ranker = RankerConfig(
        mode = config.getString("ranker.mode"),
        maxIter = config.getInt("ranker.maxIter"),
        maxDepth = config.getInt("ranker.maxDepth"),
        stepSize = config.getDouble("ranker.stepSize"),
        maturityDays = config.getInt("ranker.maturityDays"),
        labelWindowDays = config.getInt("ranker.labelWindowDays")
      ),
      paths = PathConfig(
        checkpointRoot = config.getString("paths.checkpointRoot"),
        alsModel = config.getString("paths.alsModel"),
        rankerModel = config.getString("paths.rankerModel")
      ),
      output = OutputConfig(
        publishToKafka = config.getBoolean("output.publishToKafka"),
        kafkaTopic = config.getString("output.kafkaTopic")
      ),
      tables = TableConfig(
        bronzeKafka = config.getString("tables.bronzeKafka"),
        walletInteractions = config.getString("tables.walletInteractions"),
        priceHistory = config.getString("tables.priceHistory"),
        eventCatalog = config.getString("tables.eventCatalog"),
        feedback = config.getString("tables.feedback"),
        userProfile = config.getString("tables.userProfile"),
        categoryProfile = config.getString("tables.categoryProfile"),
        eventFeatures = config.getString("tables.eventFeatures"),
        impressions = config.getString("tables.impressions"),
        rankerTraining = config.getString("tables.rankerTraining"),
        recommendations = config.getString("tables.recommendations")
      )
    )
  }
}
