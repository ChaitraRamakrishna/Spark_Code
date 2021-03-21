import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

import scala.io.Source
object Test extends App {

    val orderId=args(0).toInt
    val readorderitems=scala.io.Source.fromFile("E:\\Learning\\CCA-175\\data\\data\\retail_db\\order_items\\part-00000").getLines
    val filterorderid=readorderitems.filter(i=>i.split(",")(1).toInt==orderId)
    val getorerItemsubtotal=filterorderid.map(j=>j.split(",")(4).toFloat)
    val use_reduce = getorerItemsubtotal.reduce(_+_)
    println(use_reduce)
    val spark = SparkSession.builder().master("local[*]").appName("AddressAssignment").getOrCreate()
    val data = Seq("how old are you where are you","how are you these days")
    val rddData = sc.parallelize(data)
    //unique words in both sentences
    rddData.flatMap(line=>line.split(" ")).distinct.foreach(println)
    // words present in both the sentences
    val counts = rddData.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    counts.saveAsTextFile("WordCountSpark")

}
