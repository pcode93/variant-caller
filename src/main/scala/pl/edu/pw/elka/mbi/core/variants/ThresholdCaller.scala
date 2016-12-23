package pl.edu.pw.elka.mbi.core.variants

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.formats.avro.{GenotypeAllele, Genotype, Variant}
import pl.edu.pw.elka.mbi.core.reads.{Nucleotide, Allele}
import scala.collection.JavaConverters.seqAsJavaListConverter
import pl.edu.pw.elka.mbi.core.Timers._

object ThresholdCaller {
  def apply(variants: RDD[((String, Long), (Allele, Nucleotide))], threshold: Double = 0.2): RDD[VariantContext] = CallVariants.time {
    variants
      .map(variant => (variant._2._2, variant._2._1))
      .groupByKey()
      .map {
        case (reference, alleles) => {
            val count: Double = alleles.size
            val refAlleles = alleles.filter(_.value == reference.value)

            try {
              val call = alleles
                .filter(_.value != reference.value)
                .groupBy(_.value)
                .filter(_._2.size.toDouble / count >= threshold)
                .maxBy(_._2.size)

              val variant = Variant.newBuilder()
                .setReferenceAllele(reference.value.toString)
                .setAlternateAllele(call._1.toString)
                .setContigName(reference.pos._1)
                .setStart(reference.pos._2)
                .setEnd(reference.pos._2 + 1)
                .build()

              val genotypes = Seq(Genotype.newBuilder()
                  .setVariant(variant)
                  .setContigName(reference.pos._1)
                  .setStart(reference.pos._2)
                  .setEnd(reference.pos._2 + 1)
                  .setAlleles(List(GenotypeAllele.Alt).asJava)
                  .setReferenceReadDepth(refAlleles.size)
                  .setAlternateReadDepth(call._2.size)
                  .build())

              Some(VariantContext(variant, genotypes)): Option[VariantContext]
            } catch {
              case e: UnsupportedOperationException => None: Option[VariantContext]
            }
        }
      }
      .filter(_.isDefined)
      .map(_.get)
  }
}