package dataset

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
import org.apache.flink.api.java.utils.ParameterTool

object WordCountScala {
  def main(args: Array[String]): Unit = {
    // Set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val params = ParameterTool.fromArgs(args)

    // Make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    // Read the text file from given input path
    val inputPath = "/Users/yjkim-studio/src/flink/hands-on/data/wordCount/wc.txt"
    val text: DataSet[String] = env.readTextFile(inputPath)

    // Filter all the names starting with N
    val filtered = text.filter(new FilterFunction[String] {
      override def filter(value: String): Boolean = value.startsWith("N")
    })

    // Returns a tuple of (name, 1)
    val tokenized = filtered.map(name => (name, 1))

    // Group by tuple fields
    val counts = tokenized.groupBy(0).sum(1)

    // save the result
    val outputPath = "/Users/yjkim-studio/src/flink/hands-on/data/output/wcResult.csv"
    counts.writeAsCsv(outputPath, "\n", " ")

    // execute program
    env.execute("WordCount Example")
  }
}
