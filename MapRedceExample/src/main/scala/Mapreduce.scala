import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.network.protocol.Encoders
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window



object Mapreduce{
  val inputFile="F:\\germany_interviews\\practice\\word1.txt"
  val outFile="F:\\germany_interviews\\practice\\outWC.txt"


  def main(args:Array[String]): Unit =
  {
    val conf=new SparkConf().setMaster("local").setAppName("My App")
    val sc=new SparkContext(conf)
    val inputRDD=sc.textFile(inputFile)
    println("Total Lines:${inputRDD.count()}")

    val contentArr=inputRDD.collect()
    contentArr.foreach(println)

    val words:RDD[String]=inputRDD.flatMap(line=>line.split(" "))
    val countPerWords:RDD[(String,Int)]=words.map(word=>(word,1))
    val counts:RDD[(String,Int)]=countPerWords.reduceByKey{ case (counter,nextval)=>counter+nextval}
    FileUtils.deleteQuietly(new File(outFile))

    counts.saveAsTextFile(outFile)
    counts.foreach(println)



   // Employeewise total salery:
   val spark = SparkSession.builder().master("local[*]").appName("Practice").getOrCreate()

    val empRDD = sc.textFile("F://Screen recordings//SCALA_ADVANCED//Spark//input//employee.txt")
    val salaryRDD = sc.textFile("F://Screen recordings//SCALA_ADVANCED//Spark//input//salary.txt")
    case class Employee(empID:String,Name:String)
    case class Salery(empId:String,date:String,salery:Double)
    val res1=spark.read.option("header", true).option("delimiter", "|").schema(Encoders.product[Employee].schema).csv("F://Screen recordings//SCALA_ADVANCED//Spark//input//employee.txt")
    val res2=spark.read.option("header", true).option("delimiter", "|").schema(Encoders.product[Salery].schema).csv("F://Screen recordings//SCALA_ADVANCED//Spark//input//salary.txt")
    res1.toDF
    res2.toDF
    val resultDf = (res1.as("a").join(res2.as("c"),(col("a.empID") === col("c.empId")),"inner")).groupBy("a.empId").agg(sum("c.salery").as("sum"),max("c.salery").as("highestSalery"))



  }



  }

