package com.acme.concertrec.jobs

import com.acme.concertrec.config.AppConfig
import com.acme.concertrec.features.{EventFeatureBuilder, HistoricalInteractionBuilder, UserProfileBuilder}
import com.acme.concertrec.ml.ALSModelTrainer
import com.acme.concertrec.util.SparkSessionFactory

object TrainALSJob {
  def main(args: Array[String]): Unit = {
    val cfg = AppConfig.load()
    implicit val spark = SparkSessionFactory.getOrCreate(s"${cfg.appName}-train-als", cfg.timezone)

    val raw = spark.table(cfg.tables.walletInteractions)
    val weighted = HistoricalInteractionBuilder.build(raw, cfg.recommendation.halfLifeDays)
    val training = HistoricalInteractionBuilder.positiveForALS(weighted)

    require(!training.isEmpty, "No positive interactions available to train ALS")

    val artifacts = ALSModelTrainer.train(training, cfg.als)
    artifacts.pipelineModel.write.overwrite().save(cfg.paths.alsModel)

    UserProfileBuilder.combined(weighted, raw)
      .write.mode("overwrite").format("delta").saveAsTable(cfg.tables.userProfile)

    UserProfileBuilder.categoryProfile(weighted)
      .write.mode("overwrite").format("delta").saveAsTable(cfg.tables.categoryProfile)

    EventFeatureBuilder.build(raw)
      .write.mode("overwrite").format("delta").saveAsTable(cfg.tables.eventFeatures)
  }
}
