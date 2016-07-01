import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by hdd on 5/26/16.
  */
object CombineFile {
  def main(args: Array[String]) {
    if(args.length<3)
      throw new IllegalArgumentException("need 3 args inputFile1 inputFile2 outputFile")

    for(arg <- args)
      if(!arg.startsWith("file://"))
        throw new IllegalArgumentException("file need start with file://")
    val lineList = new ListBuffer[String]()
    for(i <- 0 until args.length-1){
      lineList ++ Source.fromFile(args(i).replace("file://","")).getLines().toList
    }
    val result = lineList.toList

    val conf = new SparkConf().setAppName("Combine").set("spark.executor.memory", "12g").set("spark.executor.cores","16").set("spark.driver.memory","2g")
    val sc = new SparkContext(conf)

    val step1 = sc.parallelize(result).map(_.split(",")).map(x=>(x.tail,x.head.toInt)).reduceByKey(_+_)
  }
}
