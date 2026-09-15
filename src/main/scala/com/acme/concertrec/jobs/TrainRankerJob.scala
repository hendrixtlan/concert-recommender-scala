package com.acme.concertrec.jobs

import com.acme.concertrec.config.AppConfig
import com.acme.concertrec.ranking.ConcertRanker
import com.acme.concertrec.util.SparkSessionFactory

object TrainRankerJob {
  def main(args: Array[String]): Unit = {
    val cfg = AppConfig.load()
    val spark = SparkSessionFactory.getOrCreate(s"${cfg.appName}-train-ranker", cfg.timezone)

    val training = spark.table(cfg.tables.rankerTraining)
      .filter("label IN (0.0, 1.0)")

    require(!training.isEmpty, "Ranker training table is empty")
    require(training.select("label").distinct().count() >= 2, "Ranker training needs both positive and negative labels")

    val model = ConcertRanker.train(training, cfg.ranker)
    model.write.overwrite().save(cfg.paths.rankerModel)
  }
}
