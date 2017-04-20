/**
  * Created by 1 on 04.04.2017.
  */

import org.apache.spark.{SparkContext, SparkConf}

object SparkCommons {
  //build the SparkConf  object at once
  lazy val conf: SparkConf = {
    new SparkConf()
      .setMaster("local[4]")
      .setAppName("rabotyagi")
      .set("spark.driver.maxResultSize", "1g")
      .set("spark.mongodb.input.uri", "mongodb://localhost/test.census")
      .set("spark.mongodb.output.uri", "mongodb://localhost/test.rabotyagi")
  }

  lazy val sc: SparkContext = SparkContext.getOrCreate(conf)
}