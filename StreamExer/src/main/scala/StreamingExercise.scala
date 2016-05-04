import javax.sql.ConnectionPoolDataSource

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hdd on 4/29/16.
  */
object StreamingExercise {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf , Seconds(5))

    val lines = ssc.socketTextStream("localhost",9999)
    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word,1))
    val wordCounts = pairs.reduceByKey(_+_)
    wordCounts.print



    val windowedWordConts = pairs.reduceByKeyAndWindow((a:Int,b:Int)=>(a+b),Seconds(30),Seconds(10))

    //KafkaUtils

    ssc.start
    ssc.awaitTermination

  }
}
