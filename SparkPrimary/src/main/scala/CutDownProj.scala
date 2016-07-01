import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hdd on 5/27/16.
  */
object CutDownProj {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("main01jar").setMaster("spark://10.0.0.102:7077").setJars(Array("/home/hdd/workspace/IBack/out/artifacts/test/test.jar")).set("spark.executor.memory", "12g").set("spark.executor.cores","16").set("spark.driver.memory","2g")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("hdfs://10.0.0.102:9000/result0603")
    System.out.println(textFile.count())

    val result = textFile.map(x=>(x,1)).reduceByKey(_+_).map(x=>(x._1.split(","),x._2)).map(x=>(x._1.last.toDouble,x)).repartition(1).sortByKey(false).map(x=>x._2._2+",0,"+x._2._1.init.length+","+x._1+","+x._2._1.init.mkString(","))
    //result.saveAsTextFile("hdfs://10.0.0.102:9000/resultout/result")

    val line = result.collect()

    println()

/*    val pw = new PrintWriter(new File("result0603.csv"))
    line.foreach(model=>pw.write(model+"\r\n"))
    pw.close()*/

  }
}
