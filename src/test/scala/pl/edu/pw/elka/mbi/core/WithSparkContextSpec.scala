package pl.edu.pw.elka.mbi.core

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FlatSpec

object WithSparkContextSpec {
  private val sc = new SparkContext(
    new SparkConf()
      .setMaster("local[4]")
      .setAppName("VariantCallerPreprocessorTest")
  )
}

abstract class WithSparkContextSpec extends FlatSpec {
  protected val sc = WithSparkContextSpec.sc
}
