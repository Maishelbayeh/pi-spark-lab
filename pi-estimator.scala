import org.apache.spark.{SparkConf, SparkContext}

object PiEstimator {
  def main(args: Array[String]): Unit = {
    require(args.length == 3, "Usage: PiEstimator samples partitions count|reduce")
    val n = args(0).toInt
    val partitions = args(1).toInt
    val method = args(2)
    require(n > 0 && n <= 100000000, "Samples must be between 1 and 100000000")
    require(partitions > 0 && partitions <= 128, "Partitions must be between 1 and 128")
    require(Set("count", "reduce").contains(method), "Method must be count or reduce")
    val sc = new SparkContext(new SparkConf().setAppName("Pi RDD Lab"))
    sc.setLogLevel("WARN")
    try {
      val started = System.nanoTime()
      val points = sc.parallelize(1 to n, partitions)
      val xy = points.map { _ =>
        (2.0 * math.random - 1.0, 2.0 * math.random - 1.0)
      }
      val inside = if (method == "count") {
        xy.filter { case (x, y) => x * x + y * y <= 1.0 }.count()
      } else {
        xy.map { case (x, y) =>
          if (x * x + y * y <= 1.0) { 1L } else { 0L }
        }.reduce(_ + _)
      }
      val elapsed = (System.nanoTime() - started) / 1e9
      val estimate = 4.0 * inside / n
      // A separate bounded sample is for the diagram, not for the estimate.
      val preview = sc.parallelize(1 to math.min(n, 800), partitions).map { _ =>
        (2.0 * math.random - 1.0, 2.0 * math.random - 1.0)
      }.collect().map { case (x, y) => s"[$x,$y]" }.mkString(",")
      println(s"""PI_RESULT={"samples":$n,"partitions":${xy.getNumPartitions},"method":"$method","inside":$inside,"estimate":$estimate,"error":${math.abs(estimate - math.Pi)},"seconds":$elapsed,"preview":[$preview]}""")
    } finally {
      sc.stop()
    }
  }
}
