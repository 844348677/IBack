import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by hdd on 16-4-25.
  */
object WordCount {
  def main(args: Array[String]) {

    val filePath = if(args.length > 0)
      args(0)
    else
      throw new IllegalArgumentException("need args filePath\r\n[local]file://...\r\n[hdfs]hdfs://...")

    val sparkConf = new SparkConf().setAppName("sparkTest").setMaster("spark://10.0.0.102:7077")//.setJars(Array("/home/hdd/workspace/test/out/artifacts/test_jar/test.jar"))
    val sc = new SparkContext(sparkConf)



    val file = if(filePath.startsWith("file://"))
        sc.parallelize(Source.fromFile(filePath.replace("file://","")).getLines().toList)
      else
        sc.textFile(filePath)

    //val file = sc.textFile(filePath)

    val result = file.flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_).map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1))
    result.collect.foreach(println)



  }
}
