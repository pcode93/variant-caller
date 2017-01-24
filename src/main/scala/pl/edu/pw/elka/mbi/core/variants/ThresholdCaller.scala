package pl.edu.pw.elka.mbi.core.variants

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.formats.avro.{Genotype, GenotypeAllele, Variant}
import pl.edu.pw.elka.mbi.core.instrumentation.Timers._
import pl.edu.pw.elka.mbi.core.model.{AlignedAllele, ReferenceAllele}

import scala.collection.JavaConverters.seqAsJavaListConverter

object ThresholdCaller {
  /**
    * Calls variants based on the thresholds.
    * First all variants are grouped by ReferenceAlleles that they map to.
    * For each group, alternate alleles are grouped together.
    *
    * Then groups of alternate alleles below the heterozygous threshold (this is determined by
    * dividing the group size by the size of all variants mapped to the ReferenceAllele)
    * get filtered out.
    *
    * Then a variant is called for the largest group of alternate alleles.
    * If the group is above the homozygous threshold, the variant is homozygous.
    * If not, the variant is heterozygous.
    * @param variants
    * @param homozygousThreshold
    * @param heterozygousThreshold
    * @return Called variants
    */
  def apply(variants: RDD[((String, Long), (AlignedAllele, ReferenceAllele))],
            homozygousThreshold: Double = 0.8,
            heterozygousThreshold: Double = 0.2): RDD[VariantContext] = CallVariants.time {
    variants
      .map(variant => (variant._2._2, variant._2._1))
      .groupByKey()
      .map {
        case (reference, mapped) => {
            val count: Double = mapped.size
            val refAlleles = mapped.filter(_.value == reference.value)

            try {
              val call = mapped
                .filter(_.value != reference.value)
                .groupBy(_.value)
                .filter(_._2.size.toDouble / count >= heterozygousThreshold)
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
                  .setAlleles(List(GenotypeAllele.Alt,
                                   if(call._2.size.toDouble / count >= homozygousThreshold)
                                     GenotypeAllele.Alt else GenotypeAllele.Ref).asJava)
                  .setReferenceReadDepth(refAlleles.size)
                  .setAlternateReadDepth(call._2.size)
                  .setReadDepth(refAlleles.size + call._2.size)
                  .setSampleId("sample")
                  .setSampleDescription("sample")
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