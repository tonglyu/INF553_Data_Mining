import java.io.PrintWriter

import org.apache.spark.sql.{DataFrame, SparkSession}

object task2 {
  def main(args:Array[String]): Unit= {
    val spark = SparkSession
      .builder()
      .appName("task2")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val reviews = spark.read.json(args(0))
    val metadata: DataFrame = spark.read.json(args(1))

    val joinTable = metadata.join(reviews, "asin")
    val brand = joinTable.groupBy("brand").avg("overall").orderBy("brand")
    val out = new PrintWriter(args(2))
    out.write("brand" + "," + "rating_avg" + "\n")
    for (row <- brand.collect()) {
      if (row.get(0) != null && row.get(0) != "" & row.get(1) != null) {
        val line = row.getString(0).filterNot(_ == ',') + "," + row.get(1) + "\n"
        out.write(line)
      }
    }
    out.close()
  }
}
