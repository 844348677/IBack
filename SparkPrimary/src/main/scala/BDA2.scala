import java.io.{File, PrintWriter}

import breeze.numerics._
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by liuh on 2016/4/11.
  */
object BDA2 {
  def main(args: Array[String]) {

    val filePath = if(args.length > 0)
      args(0)
    else
      throw new IllegalArgumentException("need args filePath\r\n[local]file://...\r\n[hdfs]hdfs://...")

    //val conf = new SparkConf().setAppName("BDA2").setMaster("spark://10.0.0.102:7077").set("spark.executor.memory", "12g").set("spark.executor.cores","16").set("spark.driver.memory","2g")
    val conf = new SparkConf().setAppName("BDA2").set("spark.executor.memory", "12g").set("spark.executor.cores","16").set("spark.driver.memory","2g")
    val sc = new SparkContext(conf)

    val file = if(filePath.startsWith("file://"))
      Source.fromFile(filePath.replace("file://","")).getLines().toList
    else
      sc.textFile(filePath).collect.toList

    val rowLength = file.length

    val bda1 = for(i <- 2 to rowLength) yield  i+""
    val bda2 = List.range(2,rowLength+1) flatMap (i => List.range(2,i) map (j => i+","+j ))
    val bda3 = for(i <- List.range(2,rowLength+1); j <- List.range(2,i); k <- List.range(2,j)) yield i+","+j+","+k

    val lines = sc.parallelize(bda2).repartition(48)

    val dim2Matrix:Array[Array[Int]] = file.map(_.split(",")).map(for(i <- _) yield i.toDouble.toInt).toArray

    val arrayY =dim2Matrix(0)      //矩阵第一行为Y值
    val barY = arrayY.sum.toDouble./(arrayY.length)///arrayY.length ///arrayY.length
    //println("平均值："+barY)
    def pickRow(index :Array[Int]) = for(i <- index) yield dim2Matrix(i-1)
    def string2int(stringArray:Array[String]) =   for(i <- stringArray) yield i.toInt  //Array[String] => Array[Int]

    val computeIscore = (matrixs:Array[Array[Int]]) =>{
      val peopleKind = pow(2,matrixs.length)
      val kindYSum = new Array[Int](peopleKind)
      val kindYSize = new Array[Int](peopleKind)
      var s:Double = 0.0

      for(i <- 0.to(matrixs(0).length-1)){
      var kindIndex:Int = 0
        for(j <- 0.to(matrixs.length-1)){
          kindIndex *= 2
          kindIndex += matrixs(j)(i)
        }
        kindYSum(kindIndex) += arrayY(i)
        kindYSize(kindIndex) += 1
      }

      for(i <- 0.to(peopleKind-1)){
        if(kindYSize(i)>0){
          val t = pow(kindYSum(i) - barY.*(kindYSize(i)),2)
          s += t
        }
      }
      s
    }

    def cutOne(intArray:Array[Int])={
      val step1 = for(i <- intArray) yield i-1
      step1.sortWith(_<_)
    }

    val happy = lines.map(_.split(",")).map(string2int(_)).map(x=>(computeIscore(pickRow(x)),x))//.filter(x=>x._1>0)
    val sorted = happy.repartition(1).sortByKey(false).map(x=>x._1+","+cutOne(x._2).mkString(","))
    val result = sorted.collect()
    //result.foreach(println)
    sorted.take(30).foreach(println)

    if(args.length > 1) {
      val outFile = args(1)
      if(outFile.startsWith("file://")){
        val pw = new PrintWriter(new File(outFile.replace("file://","")))
        result.foreach(model=>pw.write(model+"\r\n"))
        pw.close()
      }else
        sorted.saveAsTextFile(outFile)
    }

  }
}
