import org.apache.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TwitterSocketStreaming extends App {
  val conf = new SparkConf().setAppName("twitter-socket-streaming").setMaster("local[*]")
  val ssc = new StreamingContext(conf, Seconds(5))

  val lines = ssc.socketTextStream("localhost", 5000)

  val tweets = lines.map(line => line)

  tweets.print()

  ssc.start()
  ssc.awaitTermination()
}
