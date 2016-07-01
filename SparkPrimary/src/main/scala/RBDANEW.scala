import java.io.{File, PrintWriter}

import breeze.numerics._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashMap
import scala.io.Source

/**
  * Created by liuh on 2016/3/30.
  * RBDA的实现
  */
object RBDANEW {

  def main(args: Array[String]) {

    val randomSize = if(args.length > 0)
      args(0).toInt
    else
      throw new IllegalArgumentException("need Int randomSize")

    val filePath = if(args.length > 1)
      args(1)
    else
      throw new IllegalArgumentException("need args filePath\r\n[local]file://...\r\n[hdfs]hdfs://...")

    //val conf = new SparkConf()setAppName("RBDA").setMaster("spark://10.0.0.102:7077").set("spark.executor.memory", "12g").set("spark.executor.cores","16").set("spark.driver.memory","2g")
    val conf = new SparkConf().setAppName("RBDA").setMaster("spark://10.0.0.102:7077").set("spark.driver.memory","4g").set("spark.executor.memory", "12g").set("spark.executor.cores","16")
    val sc = new SparkContext(conf)

    val file = if(filePath.startsWith("file://"))
      Source.fromFile(filePath.replace("file://","")).getLines().toList
    else
      sc.textFile(filePath).collect.toList

    val rowLength = file.length
    val randomIndex = new Array[Int](randomSize)

    def randomModel():Array[Int] = {
      val randomArray = (for(i <- 1.to(16)) yield Math.random().*(rowLength-2).toInt+2).toArray
      randomArray.sorted.distinct
    }

    val lines = sc.parallelize(randomIndex).repartition(48).map(x=>randomModel)
    //val lines = sc.textFile("hdfs://10.0.0.102:9000/rbda/0418/rbdaline0418.txt").repartition(48)

    val dim2Matrix:Array[Array[Int]] = file.map(_.split(",")).map(for(i <- _) yield i.toDouble.toInt).toArray
    //val arrayY =dim2Matrix(0)
    val arrayY:Array[Double] = file.head.split(",").map(_.toDouble)
    val barY = arrayY.sum.toDouble./(arrayY.length)
    def pickRow(index :Array[Int]) = for(i <- index) yield dim2Matrix(i-1)

    val computeIscore = (matrixs:Array[Array[Int]]) =>{
      val peopleKind = pow(2,matrixs.length)
      val kindYSum = new Array[Double](peopleKind)
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

    val dropOne = (index:Array[Int],matrix:Array[Array[Int]],score:Double) =>{
      var bestScore:Double = score;
      var bestArray = index

      val aa = new HashMap[Int,String]

      for(i <- 0.to(index.length-1)){
        val indexBuffer = index.toBuffer
        indexBuffer.remove(i)
        val newIndex =indexBuffer.toArray
        val newMatrix = pickRow(newIndex)

        val newScore = computeIscore(newMatrix)

        if(newScore >= bestScore){
          bestScore = newScore
          bestArray = newIndex
        }
      }
      Tuple2(bestArray,bestScore)
    }

    val bestInArray = (index:Array[Int],matrix:Array[Array[Int]],score:Double) =>{

      var tuple2 = new Tuple2(index,score)
      var newTupleArraySize = 0
      var loopId = 0
      while(tuple2._1.length != newTupleArraySize && tuple2._1.length != 1){

        newTupleArraySize = tuple2._1.length
        tuple2 = dropOne(tuple2._1,null,tuple2._2)

        loopId += 1
      }
      //println(tuple2._1.mkString(",")+","+tuple2._2)
      tuple2
    }

    val happy = lines.map(x=>bestInArray(x,null,computeIscore(pickRow(x))))//.filter(x=>x._2>1500)

    val Array2String = (arrayValue:Array[Int])=>{
      val cutOne = for(i <- arrayValue) yield i-1
      cutOne.mkString(",")

    }

    val sortValue = happy.map(x=>(Array2String(x._1),x._2,x._1.length)).map(x=>(x,1)).reduceByKey(_+_).map(x=>(x._1._2,(x._2+","+"0"+","+x._1._3,x._1._1))).repartition(1).sortByKey(false).map(x=>x._2._1+","+x._1+","+x._2._2)
    //sortValue.take(30).foreach(println)

    if(args.length > 2) {
      val outFile = args(2)
      if(outFile.startsWith("file://")){
        val result = sortValue.collect
        val pw = new PrintWriter(new File(outFile.replace("file://","")))
        result.foreach(model=>pw.write(model+"\r\n"))
        pw.close()
      }else
        sortValue.saveAsTextFile(outFile)
    }

  }

}
