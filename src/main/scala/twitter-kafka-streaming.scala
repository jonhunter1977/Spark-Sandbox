import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object TwitterKafkaStreaming extends App {
  val conf = new SparkConf().setAppName("twitter-kafka-streaming").setMaster("local[*]")
  val ssc = new StreamingContext(conf, Seconds(5))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "192.168.10.4:9092,192.168.10.4:9093,192.168.10.4:9094",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "SparkStreamingConsumer",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("twitter-stream")
  val messages = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

  val tweets = messages.map(message => (message.key, message.value))

  tweets.print()

  ssc.start()
  ssc.awaitTermination()
}
