import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by hdd on 16-4-25.
  */
object Main2Cluster {
  def main(args: Array[String]) {

    val filePath = if(args.length > 0)
      args(0)
    else
      throw new IllegalArgumentException("need args filePath\r\n[local]file://...\r\n[hdfs]hdfs://...")


    //val conf = new SparkConf().setAppName("main01jar").setMaster("spark://10.0.0.102:7077").set("spark.executor.memory", "12g").set("spark.executor.cores","16").set("spark.driver.memory","2g")
    val conf = new SparkConf().setAppName("main01jar").set("spark.executor.memory", "12g").set("spark.executor.cores","16").set("spark.driver.memory","2g")
    val sc = new SparkContext(conf)

    val file = if(filePath.startsWith("file://"))
      Source.fromFile(filePath.replace("file://","")).getLines().toList
    else
      sc.textFile(filePath).collect.toList

    val localTrainFile = file.map(_.split(","))
    val tupleLocalTrainFile = for(i <- 0 until localTrainFile.length) yield (-i,localTrainFile(i))
    val toRDD = sc.parallelize(tupleLocalTrainFile).repartition(48)

    def loop_clustering(trainArray : Array[String]) ={
    val orderSet = trainArray.toSet
      orderSet.size match {
        case x if x < 3 => (orderSet.head.toDouble + orderSet.last.toDouble)/2
        case _ => val string2double = for(s <- trainArray) yield s.toDouble; isNotBinary(string2double.sortWith(_<_))
      }
    }

    def isNotBinary(trainArray : Array[Double]) ={

    val size = trainArray.size
      val w = new Array[Double](size - 1)
      for(i <- 0.to(size-2)){
        for(k1 <- 0 to i;k2 <- 0 to k1)
          w(i) = w(i) + (trainArray(k1)-trainArray(k2))*(trainArray(k1)-trainArray(k2))
        for(k1 <- (i+1) to (size-1);k2 <- (i+1) to k1)
          w(i) = w(i) + (trainArray(k1)-trainArray(k2))*(trainArray(k1)-trainArray(k2))
      }
      var minIndex = 0
      for(i <- 0 to (size-3)) if(w(i) >= w(i+1)) minIndex = i+1 else w(i+1) = w(i)
      val cutValue = (trainArray(minIndex) + trainArray(minIndex+1))/2
      println(cutValue)
      cutValue
    }

    val cutPoint = toRDD.map(x=>(x._1,loop_clustering(x._2))).repartition(1).sortByKey(false).collect
    cutPoint.foreach(println)

    val cutPointTail =for(i <- cutPoint.tail) yield i._2

    if(args.length > 1) {
      val outFile = args(1)
      val headY = localTrainFile.head
      val deleteY = localTrainFile.tail.map(_.map(_.toDouble))
      println("01pint.length: " + cutPointTail.length)
      println("trainMetrix.length: " + deleteY.length)
      val zipValue = cutPointTail zip deleteY

      val x01 = for(line <- zipValue) yield for(t2 <- line._2) yield if(t2 > line._1) 1 else 0
      val result01 = Array(headY.mkString(",")) ++ x01.map(_.mkString(","))
      if(outFile.startsWith("file://")) {
        val pw = new PrintWriter(new File(outFile.replace("file://","")))
        result01.foreach(l=>pw.write(l+"\r\n"))
        pw.close()
      }else{
        val resultTuple = 1.to(result01.length).reverse zip result01
        sc.parallelize(resultTuple).repartition(1).sortByKey(false).map(x=>x._2).saveAsTextFile(outFile)
      }
    }

  }

}
