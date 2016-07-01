import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by hdd on 5/27/16.
  */
object TestCountKey {
  def main(args: Array[String]) {
/*    val conf = new SparkConf().setAppName("main01jar").setMaster("spark://10.0.0.102:7077").setJars(Array("/home/hdd/workspace/IBack/out/artifacts/test/test.jar")).set("spark.executor.memory", "12g").set("spark.executor.cores","16").set("spark.driver.memory","2g")
    val sc = new SparkContext(conf)*/

/*    val file = sc.textFile("hdfs://10.0.0.102:9000/resultout/result/part-00000")
    val result = file.map(_.split(",")).map(_.head.toInt).reduce(_+_)
    System.out.println(result)*/

    val line = Source.fromFile("result0603.csv").getLines().map(x=>x.split(",")).map(x=>x(0).toDouble)
    println(line.reduce(_+_))



  }
}
