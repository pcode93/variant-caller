package pl.edu.pw.elka.mbi.core.variants

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ReferencePosition, VariantContext}
import org.bdgenomics.formats.avro.{Genotype, GenotypeAllele, Variant}
import pl.edu.pw.elka.mbi.core.instrumentation.Timers._
import pl.edu.pw.elka.mbi.core.model.{Allele, AlignedAllele, ReferenceAllele}

import scala.collection.JavaConverters.seqAsJavaListConverter

object ThresholdCaller {
  /**
    * Calls variants based on the thresholds.
    * For each position, alternate alleles are grouped together.
    *
    * Then groups of alternate alleles below the heterozygous threshold (this is determined by
    * dividing the group size by the size of all alleles mapped to the position)
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
  def apply(variants: RDD[(ReferencePosition, Iterable[Allele])],
            homozygousThreshold: Double = 0.8,
            heterozygousThreshold: Double = 0.2): RDD[VariantContext] = CallVariants.time {
    variants
      .map {
        case (pos, alleles) => {
            val ref = alleles.find {
              case allele: ReferenceAllele => true
              case _ => false
            }.get.get

            val aligned = alleles.filter {
              case allele: AlignedAllele => true
              case _ => false
            }

            val alt = aligned.filter(_.get != ref)

            if(alt.nonEmpty) {
              val refCount = aligned.count(_.get == ref)
              val count: Double = aligned.size

              val call = alt
                .groupBy(_.get)
                .filter(_._2.size.toDouble / count >= heterozygousThreshold)

                if(call.nonEmpty) {
                  val max = call.maxBy(_._2.size)

                  val variant = Variant.newBuilder()
                    .setReferenceAllele(ref)
                    .setAlternateAllele(max._1)
                    .setContigName(pos.referenceName)
                    .setStart(pos.pos)
                    .setEnd(pos.pos + 1)
                    .build()

                  val genotypes = Seq(Genotype.newBuilder()
                    .setVariant(variant)
                    .setContigName(pos.referenceName)
                    .setStart(pos.pos)
                    .setEnd(pos.pos + 1)
                    .setAlleles(List(
                      GenotypeAllele.Alt,
                      if (max._2.size.toDouble / count >= homozygousThreshold)
                        GenotypeAllele.Alt
                      else GenotypeAllele.Ref
                    ).asJava)
                    .setReferenceReadDepth(refCount)
                    .setAlternateReadDepth(max._2.size)
                    .setReadDepth(refCount + max._2.size)
                    .setSampleId("sample")
                    .setSampleDescription("sample")
                    .build())

                  Some(VariantContext(variant, genotypes))
                } else {
                  None
                }
            } else {
              None
            }
        }
      }
      .filter(_.isDefined)
      .map(_.get)
  }
}