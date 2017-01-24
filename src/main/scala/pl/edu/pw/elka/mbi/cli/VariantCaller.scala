package pl.edu.pw.elka.mbi.cli

import java.io.PrintWriter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rdd.variation.VariantContextRDD
import org.bdgenomics.formats.avro.{AlignmentRecord, Sample}
import org.bdgenomics.utils.instrumentation.Metrics
import pl.edu.pw.elka.mbi.core.instrumentation.Timers._
import pl.edu.pw.elka.mbi.core.model.ReferenceAllele
import pl.edu.pw.elka.mbi.core.preprocessing.Preprocessor
import pl.edu.pw.elka.mbi.core.reads.{ReferenceSequence, VariantDiscovery}
import pl.edu.pw.elka.mbi.core.variants.ThresholdCaller

object VariantCaller {
  private val ARGS = Map(
    "DEBUG" -> ((arg: String) => arg.toBoolean),
    "reference" -> ((arg: String) => arg),
    "alignment" -> ((arg: String) => arg),
    "out" -> ((arg: String) => arg),
    "mapqThreshold" -> ((arg: String) => arg),
    "homozygousThreshold" -> ((arg: String) => arg.toInt),
    "heterozygousThreshold" -> ((arg: String) => arg.toInt)
    )
  private var args = Map[String, Any]()

  def debug(text: String) = {
    if(args.get("DEBUG").isDefined) {
      println(text)
    }
  }

  def main(args: Array[String]) {
    this.args = getArgs(args)

    if (this.args.get("reference").isEmpty ||
        this.args.get("alignment").isEmpty ||
        this.args.get("out").isEmpty) {
      System.err.println("Specify reference file, read file and vcf file")
      System.exit(1)
    }

    this.args = getArgs(args)

    val conf = new SparkConf()
                     .setAppName("Variant Caller")
                     .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                     .set("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
                     .set("spark.kryo.referenceTracking", "true")
                     //.set("spark.kryo.registrationRequired", "true")

    val sc = new SparkContext(conf)

    Metrics.initialize(sc)

    val alignments: AlignmentRecordRDD = LoadReads.time {
      sc.loadAlignments(this.args("alignment").toString)
    }
    val sequence: NucleotideContigFragmentRDD = LoadReference.time {
      sc.loadSequences(this.args("reference").toString)
    }

    val rdd: RDD[AlignmentRecord] = new Preprocessor(alignments)
                                          //.realignIndels()
                                          //.filterByMappingQuality(0)
                                          .getReads

    val reference: RDD[((String, Long), ReferenceAllele)] = ReferenceSequence(sequence.rdd)

    debug("Mapped reference sequence")

    val observations = VariantDiscovery(rdd, reference)

    debug("Joined read alleles with reference alleles.")

    val variants = ThresholdCaller(observations)

    debug("Called variants")

    VariantContextRDD(variants,
                      sequence.sequences,
                      Seq(new Sample("sample","sample",null))).saveAsVcf(this.args("out").toString, asSingleFile = true)

    Metrics.print(new PrintWriter(System.out), None)
  }

  private def getArgs(args: Array[String]) = {
    args
      .map(_.split('='))
      .foldLeft(Map[String, Any]()) {
        case (map, arg) => map + (arg(0) -> ARGS(arg(0))(arg(1)))
      }
  }
}
