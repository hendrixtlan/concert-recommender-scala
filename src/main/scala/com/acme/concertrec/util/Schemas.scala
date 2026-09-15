package com.acme.concertrec.util

import org.apache.spark.sql.types._

object Schemas {
  val walletInteraction: StructType = StructType(Seq(
    StructField("user_id", StringType, nullable = false),
    StructField("event_id", StringType, nullable = false),
    StructField("ticket_id", StringType, nullable = true),
    StructField("event_type", StringType, nullable = false),
    StructField("category", StringType, nullable = true),
    StructField("subcategory", StringType, nullable = true),
    StructField("artist", StringType, nullable = true),
    StructField("venue_id", StringType, nullable = true),
    StructField("market", StringType, nullable = true),
    StructField("ticket_price", DoubleType, nullable = true),
    StructField("currency", StringType, nullable = true),
    StructField("event_ts", TimestampType, nullable = false)
  ))

  val priceEvent: StructType = StructType(Seq(
    StructField("event_id", StringType, nullable = false),
    StructField("price", DoubleType, nullable = false),
    StructField("currency", StringType, nullable = true),
    StructField("observed_ts", TimestampType, nullable = false)
  ))

  val eventCatalog: StructType = StructType(Seq(
    StructField("event_id", StringType, nullable = false),
    StructField("category", StringType, nullable = true),
    StructField("subcategory", StringType, nullable = true),
    StructField("artist", StringType, nullable = true),
    StructField("venue_id", StringType, nullable = true),
    StructField("market", StringType, nullable = true),
    StructField("event_start_ts", TimestampType, nullable = false),
    StructField("current_price", DoubleType, nullable = true),
    StructField("currency", StringType, nullable = true),
    StructField("status", StringType, nullable = true),
    StructField("updated_ts", TimestampType, nullable = false)
  ))

  val recommendationFeedback: StructType = StructType(Seq(
    StructField("recommendation_id", StringType, nullable = false),
    StructField("user_id", StringType, nullable = false),
    StructField("event_id", StringType, nullable = false),
    StructField("action", StringType, nullable = false),
    StructField("position", IntegerType, nullable = true),
    StructField("score", DoubleType, nullable = true),
    StructField("feedback_ts", TimestampType, nullable = false)
  ))
}
