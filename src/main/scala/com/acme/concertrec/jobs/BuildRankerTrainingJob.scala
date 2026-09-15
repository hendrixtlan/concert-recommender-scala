package com.acme.concertrec.jobs

import com.acme.concertrec.config.AppConfig
import com.acme.concertrec.ranking.RankerTrainingBuilder
import com.acme.concertrec.util.SparkSessionFactory

/**
  * Expects an impression feature table/view named
  * concert_rec.features.recommendation_impressions by default. Each row must
  * contain recommendation_id, user_id, event_id and the same features used at
  * scoring time. Persisting features at impression time prevents training/serving skew.
  */
object BuildRankerTrainingJob {
  def main(args: Array[String]): Unit = {
    val cfg = AppConfig.load()
    val spark = SparkSessionFactory.getOrCreate(s"${cfg.appName}-build-ranker-training", cfg.timezone)

    val impressionTable = sys.props.getOrElse("impression.table", cfg.tables.impressions)

    val impressions = spark.table(impressionTable)
    val feedback = spark.table(cfg.tables.feedback)
    val training = RankerTrainingBuilder.fromFeedback(
      impressions,
      feedback,
      cfg.ranker.maturityDays,
      cfg.ranker.labelWindowDays
    )

    training.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(cfg.tables.rankerTraining)
  }
}
