/**
 *                                 Test Assignment - II
 *  -------------------------------------------------------------------------------------------------
 *  Problem Statement : Which are the top 100 recipient email addresses? (An email sent to N recipients would could N times - count “cc” as 50%)
 *  Solution : Scala program for tracking the issue.
 *  -------------------------------------------------------------------------------------------------
 *  
 *  @ Author : Samrat Paul
 *  @ Date   : Tue Sep 27,2016.
 *  -------------------------------------------------------------------------------------------------
 */
package com.sam.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.math.random
import org.apache.spark._

/**
 * Problem Statement- Find out Top 100 Email receipients from the mail list. Receipient ranking weitage 
 * To/CC = 2:1
 * @author sam
 */
object EmailRecipient {
  def main(args: Array[String]) {
   // create a configuration for sparkcontext
  val conf = new SparkConf().setAppName("EmailRecipient").setMaster("local")
  
  // initialize a context ......
  val sparkContext= new SparkContext(conf)
  // read the input file ....
  val inputRDD = sparkContext.textFile("mail.txt,mail1.txt")
  
  // extracting the To List and CC List
  val extractToRDD =inputRDD.filter { line => line.contains("To:") }
  val extractCcRDD =inputRDD.filter { line => line.contains("Cc:") }
 
  // applying regex pattern to filter out email adress from the list.
  val regexPattern = """(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}\b""".r
  //to list
  val emaliTo=extractToRDD.flatMap { regexPattern.findAllIn _ }
  // cc list
  val emaliCc=extractCcRDD.flatMap { regexPattern.findAllIn _ }
  
  // adding to map and with scoring To: candidate with 1 and Cc: candidate with .5
  val ToRecepients= emaliTo.map(word=>(word,1)).reduceByKey{case (x, y) => x + y}
  val CcRecepients= emaliTo.map(word=>(word,.5)).reduceByKey{case (x, y) => x + y}
 
  // join two RDD based on key ..i.e email id here
  val result = ToRecepients.join(CcRecepients).map { 
    case (key, (x, y)) => (key, x+y) }
  // sorting the email id based on scoring....
  val resut=result.sortBy(_._2,ascending = false)
  //storing it to text file
  resut.saveAsTextFile("result.txt")
 
  }
}