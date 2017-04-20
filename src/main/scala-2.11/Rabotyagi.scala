import com.mongodb.spark.MongoSpark
import org.bson._
import org.bson.types.ObjectId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import scala.collection.JavaConverters._

/**
  * Created by 1 on 13.04.2017.
  */


//case class Person (
//  race: String,
//  fnlwgt: Long,
//  rabotyaga: Boolean
//)

case class Rabotyaga(
                      race: String,
                      percent: String,
                      rabotyagi: Long,
                      fnlwgt: Long
                    )

//object Person {
//  implicit def reader(doc: BsonDocument): Person = {
//    Person(
//      doc.getString("race").getValue,
//      doc.getInt32("fnlwgt").getValue.toLong,
//      doc.getString("anual-income").getValue == "<=50K"
//    )
//  }
//}

object Robotyaga {
  implicit def writer(rabotyaga: Rabotyaga): BsonDocument = new BsonDocument(
    List(
      new BsonElement("race", new BsonString(rabotyaga.race)),
      new BsonElement("percent", new BsonString(rabotyaga.percent)),
      new BsonElement("rabotyagi", new BsonInt64(rabotyaga.rabotyagi)),
      new BsonElement("fnlwgt", new BsonInt64(rabotyaga.fnlwgt))
    ).asJava
  )
}

object Rabotyagi {

  val ss: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("rabotyagi")
    .config("spark.driver.maxResultSize", "1g")
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/test.census")
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/test.rabotyagi")
    .getOrCreate()

  import ss.implicits._

  def main(args: Array[String]) {
    //    MongoSpark.load[Person](ss)
    def scr = MongoSpark.load(ss).toDF().cache()
    val countries = Array("United-States", "England", "Scotland", "Ireland", "France", "Germany", "Canada")
    countries.foreach { country =>
      val rabotyagi: RDD[Rabotyaga] = scr.filter(per => per.getAs("native-country") == country)
        .map { per =>
          if (per.getAs[String]("anual-income") == "<=50K")
            per.getAs[String]("race") -> (per.getAs[Int]("fnlwgt").toLong, per.getAs[Int]("fnlwgt").toLong)
          else
            per.getAs[String]("race") -> (0L, per.getAs[Int]("fnlwgt").toLong)
        }
        .rdd
        .reduceByKey { case ((t11, t12), (t21, t22)) => (t11 + t21, t12 + t22) }
        .map { case (race, (t1, t2)) => Rabotyaga(race, 100 * t1 / t2 + "%", t1, t2) }
      println(country)
      rabotyagi.foreach { case Rabotyaga(race, perc, t1, t2) => println(f"$race: $t1 of $t2 are rabotyagi, $perc") }
      val wc = WriteConfig(Map("uri" -> ("mongodb://localhost:27017/test.rabotyagi_" + country)))
      MongoSpark.save(rabotyagi.toDF.write.mode("overwrite"), wc)
    }

  }
}

//val rabotyagi2 = rabotyagi.aggregate(Map[String, (Long, Long)]("White" -> (0l, 0l), "Black" -> (0l, 0l)))(seqOp, comOp)
//      .map { case (k, (v1, v2)) => (k, (v1 / v2) * 100) }
//    val whites = rabotyagi2("White")
//    val blacks = rabotyagi2("Black")
//    println(f"white rabotyagi:  $whites%%")
//    println(f"black rabotyagi: $blacks%%")

//  def seqOp(acc: Map[String, (Long, Long)], per: Person): Map[String, (Long, Long)] = {
//    val prev = acc(per.race)
//    val count1 = if (per.income == "<=50K") prev._1 + per.fnlwgt
//    else prev._1
//    val count2 = prev._2 + per.fnlwgt
//    acc.updated(per.race, (count1, count2))
//  }
//
//  def comOp(acc1: Map[String, (Long, Long)], acc2: Map[String, (Long, Long)]): Map[String, (Long, Long)] =
//    acc1.map { case (k, (v1, v2)) => (k, (v1 + acc2(k)._1, v2 + acc2(k)._2)) }
