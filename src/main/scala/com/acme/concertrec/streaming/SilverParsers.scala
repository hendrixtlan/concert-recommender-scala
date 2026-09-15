package com.acme.concertrec.streaming

import com.acme.concertrec.util.Schemas
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SilverParsers {
  def walletInteractions(bronze: DataFrame): DataFrame = {
    val inferredEventType =
      when(col("topic") === "wallet.ticket.purchased", lit("ticket_purchased"))
        .when(col("topic") === "wallet.ticket.redeemed", lit("ticket_redeemed"))
        .when(col("topic") === "wallet.ticket.refunded", lit("ticket_refunded"))
        .when(col("topic") === "wallet.ticket.transferred", lit("ticket_transferred"))
        .otherwise(lit(null).cast("string"))

    bronze
      .filter(col("topic").startsWith("wallet.ticket."))
      .select(from_json(col("message_value"), Schemas.walletInteraction).alias("x"), col("topic"))
      .selectExpr("x.*", "topic")
      .withColumn("topic_event_type", inferredEventType)
      .withColumn("event_type", coalesce(col("event_type"), col("topic_event_type")))
      .drop("topic_event_type")
      .filter(col("user_id").isNotNull && col("event_id").isNotNull && col("event_ts").isNotNull)
  }

  def prices(bronze: DataFrame): DataFrame = {
    bronze
      .filter(col("topic") === "events.price.changed")
      .select(from_json(col("message_value"), Schemas.priceEvent).alias("x"))
      .select("x.*")
      .filter(col("event_id").isNotNull && col("price").isNotNull && col("observed_ts").isNotNull)
  }

  def catalog(bronze: DataFrame): DataFrame = {
    bronze
      .filter(col("topic") === "events.catalog")
      .select(from_json(col("message_value"), Schemas.eventCatalog).alias("x"))
      .select("x.*")
      .filter(col("event_id").isNotNull && col("event_start_ts").isNotNull)
  }

  def feedback(bronze: DataFrame): DataFrame = {
    bronze
      .filter(col("topic") === "recommendations.feedback")
      .select(from_json(col("message_value"), Schemas.recommendationFeedback).alias("x"))
      .select("x.*")
      .filter(col("user_id").isNotNull && col("event_id").isNotNull && col("feedback_ts").isNotNull)
  }
}
