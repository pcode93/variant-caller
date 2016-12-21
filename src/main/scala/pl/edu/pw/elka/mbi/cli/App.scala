package pl.edu.pw.elka.mbi.cli

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.formats.avro.AlignmentRecord
import pl.edu.pw.elka.mbi.core.preprocessing.Preprocessor
import pl.edu.pw.elka.mbi.core.reads.{Reference, BaseObservations, Nucleotide}

object App {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("at least one argument required, e.g. foo.sam")
      System.exit(1)
    }

    val conf = new SparkConf()
                     .setAppName("Count Alignments")
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

    val reference: RDD[((String, Long), Nucleotide)] = Reference(sequence.rdd)
    val observations = BaseObservations(rdd, reference)
      //rdd.foreach(println)
    observations.foreach(println)
//    rdd.map(rec => if (rec.getReadMapped) rec.getContigName else "unmapped")
//      .map(contigName => (contigName, 1))
//      .reduceByKey(_ + _)
//      .foreach(println)
  }
}
