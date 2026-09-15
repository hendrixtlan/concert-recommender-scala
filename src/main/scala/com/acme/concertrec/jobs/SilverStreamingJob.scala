package com.acme.concertrec.jobs

import com.acme.concertrec.config.AppConfig
import com.acme.concertrec.streaming.SilverParsers
import com.acme.concertrec.util.SparkSessionFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

object SilverStreamingJob {
  private def write(df: DataFrame, table: String, checkpoint: String): StreamingQuery = {
    df.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpoint)
      .toTable(table)
  }

  def main(args: Array[String]): Unit = {
    val cfg = AppConfig.load()
    val spark = SparkSessionFactory.getOrCreate(s"${cfg.appName}-silver", cfg.timezone)
    val bronze = spark.readStream.table(cfg.tables.bronzeKafka)

    val queries = Seq(
      write(
        SilverParsers.walletInteractions(bronze),
        cfg.tables.walletInteractions,
        s"${cfg.paths.checkpointRoot}/silver-wallet"
      ),
      write(
        SilverParsers.prices(bronze),
        cfg.tables.priceHistory,
        s"${cfg.paths.checkpointRoot}/silver-price"
      ),
      write(
        SilverParsers.catalog(bronze),
        cfg.tables.eventCatalog,
        s"${cfg.paths.checkpointRoot}/silver-catalog"
      ),
      write(
        SilverParsers.feedback(bronze),
        cfg.tables.feedback,
        s"${cfg.paths.checkpointRoot}/silver-feedback"
      )
    )

    spark.streams.awaitAnyTermination()
    queries.foreach(_.stop())
  }
}
