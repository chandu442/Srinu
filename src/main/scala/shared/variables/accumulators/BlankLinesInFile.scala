package shared.variables.accumulators

import org.apache.spark.sql.SparkSession

object BlankLinesInFile {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("spark accumulators")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val blankLinesInFile = sc.accumulator(0, "blank lines findout")

    val fileRDD = sc.textFile("src/test/datasets/accum_test.txt", 2)

    fileRDD.foreach { line => if (line.length() == 0) blankLinesInFile += 1 }

    println(s"\tBlank Lines =${blankLinesInFile.value}\t")

    println("=========lost line================================")

  }

}