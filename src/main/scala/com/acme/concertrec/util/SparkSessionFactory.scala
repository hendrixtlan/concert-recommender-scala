package com.acme.concertrec.util

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {
  def getOrCreate(appName: String, timezone: String): SparkSession = {
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", timezone)
    spark
  }
}
