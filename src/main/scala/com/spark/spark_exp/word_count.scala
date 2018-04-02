package com.spark.spark_exp
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object word_count {
  def main(args:Array[String])
  {
    val conf = new SparkConf()
    .setAppName("WordCount")
    .setMaster("local")
    
    if(args.length <2){
      println("in suffiiceint arguments.....")
    }
      
    val sc= new SparkContext(conf)
    
    val textfile =sc.textFile(args(0))
    
    val words= textfile.flatMap(line =>line.split(" "))
    
    val eachword = words.map(word=>(word,1))
    
    val wordcnt = eachword.reduceByKey(_+_)
    
    val wordcntsorted = wordcnt.sortByKey()
    
    wordcntsorted.saveAsTextFile(args(1))

   println("Om NamahShivaya"); 
  }
}