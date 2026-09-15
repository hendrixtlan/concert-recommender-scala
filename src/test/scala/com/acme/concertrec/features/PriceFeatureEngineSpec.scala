package com.acme.concertrec.features

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class PriceFeatureEngineSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[2]").appName("PriceFeatureEngineSpec").getOrCreate()
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  test("hard budget rule removes events above lifetime average") {
    import spark.implicits._

    val candidates = Seq(
      ("U1", "E1", 1800.0),
      ("U1", "E2", 2200.0)
    ).toDF("user_id", "event_id", "catalog_current_price")

    val users = Seq(("U1", 2000.0)).toDF("user_id", "lifetime_avg_price")

    val prices = Seq(
      ("E1", 1800.0),
      ("E2", 2200.0)
    ).toDF("event_id", "observed_current_price")
      .withColumn("price_1d_ago", org.apache.spark.sql.functions.lit(null).cast("double"))
      .withColumn("price_7d_ago", org.apache.spark.sql.functions.lit(null).cast("double"))
      .withColumn("price_volatility_7d", org.apache.spark.sql.functions.lit(0.0))
      .withColumn("price_change_1d", org.apache.spark.sql.functions.lit(0.0))
      .withColumn("price_change_7d", org.apache.spark.sql.functions.lit(0.0))
      .withColumn("latest_ts", org.apache.spark.sql.functions.current_timestamp())

    val eligible = PriceFeatureEngine.attachUserFeatures(candidates, users, prices)
    assert(eligible.select("event_id").as[String].collect().toSet == Set("E1"))
  }
}
