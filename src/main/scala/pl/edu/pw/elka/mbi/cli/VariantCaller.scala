package pl.edu.pw.elka.mbi.cli

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rdd.variation.VariantContextRDD
import org.bdgenomics.formats.avro.AlignmentRecord
import pl.edu.pw.elka.mbi.core.preprocessing.Preprocessor
import pl.edu.pw.elka.mbi.core.reads.{Nucleotide, ReferenceSequence, VariantDiscovery}
import pl.edu.pw.elka.mbi.core.variants.ThresholdCaller
import pl.edu.pw.elka.mbi.core.Timers._

object VariantCaller {
  val DEBUG = true

  def debug(header: String, text: RDD[String]) = {
    if(DEBUG) {
      println(header)
      text foreach println
    }
  }

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Specify reference file, read file and vcf file")
      System.exit(1)
    }

    val conf = new SparkConf()
                     .setAppName("Variant Caller")
                     .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                     .set("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
                     .set("spark.kryo.referenceTracking", "true")
                     //.set("spark.kryo.registrationRequired", "true")

    val sc = new SparkContext(conf)
    val alignments: AlignmentRecordRDD = LoadReference.time {
      sc.loadAlignments(args(0))
    }
    val sequence: NucleotideContigFragmentRDD = LoadReads.time {
      sc.loadSequences(args(1))
    }

    val rdd: RDD[AlignmentRecord] = new Preprocessor(alignments)
                                          //.realignIndels()
                                          .filterByBaseQuality(0)
                                          .filterByMappingQuality(0)
                                          .reads

    val reference: RDD[((String, Long), Nucleotide)] = ReferenceSequence(sequence.rdd)

    debug("----------------REFERENCE SEQUENCE---------------------", reference.map(_.toString))

    val observations = VariantDiscovery(rdd, reference)

    debug("----------------COMPARISONS---------------------", observations.map(_.toString))

    val variants = ThresholdCaller(observations)

    debug("----------------CALLED VARIANTS---------------------", variants.map(_.toString))

    VariantContextRDD(variants, sequence.sequences, Seq()).saveAsVcf(args(2), asSingleFile = true)
  }
}
