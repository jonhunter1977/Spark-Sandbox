import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jon on 28/06/17.
  */
object Spam {
  def main(args: Array[String]): Unit = {
    val spamFile = "/home/jon/spark/spam-data/data/spam.csv"

    val conf = new SparkConf().setAppName("Spam Reporter")
    conf.setMaster("local")
    conf.setSparkHome("/usr/local/spark")

    val sc = new SparkContext(conf)

    val spamData = sc.textFile(spamFile, 2).cache()
    val filteredData = spamData.map(line => getData(line))
      .filter(data => getSpams(data))

    var numSpams = filteredData.count()

    filteredData.repartition(1).saveAsTextFile("outfile")

    println(s"=================================START OUTPUT=================================")
    println(s"There are $numSpams spam texts")
    println(s"=================================END OF OUTPUT=================================")

    sc.stop()
  }

  def getData(line:String): String = {
    val splitLine = line.split(",")
    if (splitLine.length > 1) return splitLine(0) + "," + splitLine(1)
    return null
  }

  def getSpams(line:String): Boolean = {
    if (line == null) return false
    val splitLine = line.split(",")
    return splitLine(0) == "spam"
  }

}
