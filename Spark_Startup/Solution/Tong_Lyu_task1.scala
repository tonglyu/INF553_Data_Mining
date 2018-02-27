import org.apache.spark.sql.SparkSession
import java.io._

object task1 {
  def main(args:Array[String]): Unit= {
    val spark = SparkSession
      .builder()
      .appName("task1")
      .master("local[2]")
      .config("spark.some.config.option","some-value")
      .getOrCreate()

    val inReview = spark.read.json(args(0))
    val avgRating = inReview.groupBy("asin").avg("overall").orderBy("asin")

    val out = new PrintWriter(args(1))
    out.write("asin"+","+"rating_avg"+"\n")
    for (row <- avgRating.collect()) {
      val line = row.get(0) + ","+row.get(1)+"\n"
      out.write(line)
    }
    out.close()
  }
}
