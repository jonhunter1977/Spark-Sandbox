import org.apache.spark._
import org.apache.spark.streaming._
import Spam.{getData, getSpams}

object SpamStreaming extends App {
  val conf = new SparkConf().setAppName("spam-streaming").setMaster("local[*]")
  val ssc = new StreamingContext(conf, Seconds(5))

  val lines = ssc.socketTextStream("localhost", 9999)

  val filteredData = lines.map(line => getData(line))
    .filter(data => getSpams(data))

  var numSpams = filteredData.count()

  numSpams.print()

  ssc.start()
  ssc.awaitTermination()
}

