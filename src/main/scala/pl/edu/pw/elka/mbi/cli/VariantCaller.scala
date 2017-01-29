package pl.edu.pw.elka.mbi.cli

import java.io.PrintWriter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.ReferencePosition
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rdd.variation.VariantContextRDD
import org.bdgenomics.formats.avro.{AlignmentRecord, Sample}
import org.bdgenomics.utils.instrumentation.Metrics
import org.kohsuke.args4j.{CmdLineParser, Argument, Option}
import pl.edu.pw.elka.mbi.core.instrumentation.Timers._
import pl.edu.pw.elka.mbi.core.model.Allele
import pl.edu.pw.elka.mbi.core.preprocessing.Preprocessor
import pl.edu.pw.elka.mbi.core.reads.{ReferenceSequence, VariantDiscovery}
import pl.edu.pw.elka.mbi.core.variants.ThresholdCaller

object VariantCaller {

  @Argument(usage = "Path to the reference file", metaVar = "Reference", required = true, index = 0)
  private var referenceFile: String = _

  @Argument(usage = "Path to the alignment file", metaVar = "Alignment", required = true, index = 1)
  private var alignmentFile: String = _

  @Argument(usage = "Path to the output vcf file", metaVar = "Output", required = true, index = 2)
  private var outputFile: String = _

  @Option(name = "-debug", usage = "Turn on debug logs")
  private var debug: Boolean = false

  @Option(name = "-homozygousThreshold", usage = "Set the homozygous threshold")
  private var homozygousThreshold: Double = 0.8

  @Option(name = "-heterozygousThreshold", usage = "Set the heterozygous threshold")
  private var heterozygousThreshold: Double = 0.2

  @Option(name = "-mapq", usage = "Filter reads based on mapping quality")
  private var mapqFilter: Int = 0

  def debugLog(text: String) = {
    if(debug) {
      println(text)
    }
  }

  def debugRDD(text: RDD[String]) = {
    text foreach debugLog
  }

  def apply(args: Array[String]) = {
    new CmdLineParser(this).parseArgument(args: _*)

    val conf = new SparkConf()
                      .setAppName("Variant Caller")
                      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                      .set("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
                      .set("spark.kryo.referenceTracking", "true")
                    //.set("spark.kryo.registrationRequired", "true")

    val sc = new SparkContext(conf)

    Metrics.initialize(sc)

    val alignments: AlignmentRecordRDD = LoadReads.time {
      sc.loadAlignments(alignmentFile)
    }
    val sequence: NucleotideContigFragmentRDD = LoadReference.time {
      sc.loadSequences(referenceFile)
    }

    val reads: RDD[AlignmentRecord] = new Preprocessor(alignments)
                                            //.realignIndels()
                                            //.filterByMappingQuality(0)
                                            .getReads

    val reference: RDD[(ReferencePosition, Allele)] = ReferenceSequence(sequence.rdd)

    val discovered = VariantDiscovery(reads, reference)

    val variants = ThresholdCaller(discovered)

    VariantContextRDD(
      variants,
      sequence.sequences,
      Seq(new Sample("sample", "sample", null))
    ).saveAsVcf(outputFile, asSingleFile = true)

    Metrics.print(new PrintWriter(System.out), None)
  }

  def main(args: Array[String]) {
    VariantCaller(args)
  }
}
