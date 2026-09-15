package com.acme.concertrec.ranking

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class DiversityRerankerSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[2]").appName("DiversityRerankerSpec").getOrCreate()
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  test("reranker limits repeated artist") {
    import spark.implicits._
    val scored = Seq(
      ("U1", "E1", "Muse", "Rock", 0.99),
      ("U1", "E2", "Muse", "Rock", 0.98),
      ("U1", "E3", "Radiohead", "Rock", 0.97),
      ("U1", "E4", "Justice", "Electronic", 0.96)
    ).toDF("user_id", "event_id", "artist", "category", "recommendation_score")

    val out = DiversityReranker.rerank(scored, topK = 3, maxPerArtist = 1, maxPerCategory = 2)
    assert(out.filter("artist = 'Muse'").count() == 1)
    assert(out.count() == 3)
  }
}
