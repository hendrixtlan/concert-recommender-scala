package com.acme.concertrec.features

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class HistoricalInteractionBuilderSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("HistoricalInteractionBuilderSpec")
      .getOrCreate()
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  test("recent redeemed event has stronger signal than old redeemed event") {
    import spark.implicits._
    val input = Seq(
      ("U1", "E1", "ticket_redeemed", "2026-09-14 00:00:00"),
      ("U1", "E2", "ticket_redeemed", "2024-09-14 00:00:00")
    ).toDF("user_id", "event_id", "event_type", "event_ts_raw")
      .selectExpr("user_id", "event_id", "event_type", "cast(event_ts_raw as timestamp) event_ts")

    val out = HistoricalInteractionBuilder.build(
      input,
      halfLifeDays = 365.0,
      asOfTsExpr = "timestamp'2026-09-15 00:00:00'"
    )

    val strengths = out.select("event_id", "interaction_strength")
      .as[(String, Double)].collect().toMap

    assert(strengths("E1") > strengths("E2"))
    assert(strengths("E1") > 3.9)
    assert(strengths("E2") < 1.1)
  }
}
