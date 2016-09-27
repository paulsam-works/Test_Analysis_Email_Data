package com.sam.test


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.math.random
import org.apache.spark._

/**
 * @author sam
 */
object AvgWordCount {
  def main(args: Array[String]) {
  // create a configuration for sparkcontext
  val conf = new SparkConf().setAppName("AvgWordCount").setMaster("local")
  
  // initialize a context ......
  val sparkContext= new SparkContext(conf)
  
  //loading the inputfile into RDD
  val inputRDD = sparkContext.textFile("mail.txt,mail1.txt")
  //val inputRDD = sparkContext.wholeTextFiles("/inputdata")
  // filtering out email header part from the mail
  val input =inputRDD.filter { line => !line.contains("To:") }
                      .filter { line => !line.contains("From:") }
                      .filter { line => !line.contains("Cc:") }
                      .filter { line => !line.contains("Subject:") }
  input.take(5).foreach(println)
  // split the line with words and for each word aggregating count and length
  val words = input.flatMap(line1=>line1.split(" "))
  // a unique key has been used both map - WordLengthAvg
  val wordCount= words.map(word=>("WordLengthAvg",1)).reduceByKey{case (x, y) => x + y}
  val wordLength= words.map(word=>("WordLengthAvg",word.length())).reduceByKey{case (x, y) => x + y}
  // joinging two RDDs for geting the average
  val result = wordLength.join(wordCount).map { 
  case (key, (x, y)) => (key, x/y) 
  }
  //saving the output in text file
  result.saveAsTextFile("EmailAvgWordCountResult.txt")
  
  sparkContext.stop()
}
}