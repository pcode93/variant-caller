package pl.edu.pw.elka.mbi.cli

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rdd.variation.VariantContextRDD
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.utils.cli.SaveArgs
import pl.edu.pw.elka.mbi.core.preprocessing.Preprocessor
import pl.edu.pw.elka.mbi.core.reads.{ReferenceSequence, VariantDiscovery, Nucleotide}
import pl.edu.pw.elka.mbi.core.variants.ThresholdCaller

object VariantCaller {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("at least one argument required, e.g. foo.sam")
      System.exit(1)
    }

    val conf = new SparkConf()
                     .setAppName("Variant Caller")
                     .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                     .set("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator")
                     .set("spark.kryo.referenceTracking", "true")
                     //.set("spark.kryo.registrationRequired", "true")

    val sc = new SparkContext(conf)
    val alignments: AlignmentRecordRDD = sc.loadAlignments(args(0))
    val sequence: NucleotideContigFragmentRDD = sc.loadSequences(args(1))

    val rdd: RDD[AlignmentRecord] = new Preprocessor(alignments)
                                          //.realignIndels()
                                          .filterByBaseQuality(0)
                                          .filterByMappingQuality(0)
                                          .reads

    val reference: RDD[((String, Long), Nucleotide)] = ReferenceSequence(sequence.rdd)
    println("----------------REFERENCE SEQUENCE---------------------")
    reference.foreach(println)

    val observations = VariantDiscovery(rdd, reference)
    println("----------------COMPARISONS---------------------")
    observations.foreach(println)

    val variants = ThresholdCaller(observations)
    println("----------------CALLED VARIANTS---------------------")
    variants.foreach(println)

    VariantContextRDD(variants, sequence.sequences, Seq()).saveAsVcf("aritificial.vcf", asSingleFile = true)
  }
}
