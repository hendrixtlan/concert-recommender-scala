package com.acme.concertrec.jobs

import com.acme.concertrec.config.AppConfig
import com.acme.concertrec.streaming.KafkaIngestion
import com.acme.concertrec.util.SparkSessionFactory

object StreamingIngestionJob {
  def main(args: Array[String]): Unit = {
    val cfg = AppConfig.load()
    val spark = SparkSessionFactory.getOrCreate(s"${cfg.appName}-kafka-ingestion", cfg.timezone)

    val raw = KafkaIngestion.rawStream(spark, cfg.kafka)

    raw.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", s"${cfg.paths.checkpointRoot}/bronze-kafka")
      .toTable(cfg.tables.bronzeKafka)
      .awaitTermination()
  }
}
