package pl.edu.pw.elka.mbi.core.variants

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.formats.avro.{Genotype, Variant}
import pl.edu.pw.elka.mbi.core.reads.{Nucleotide, Allele}

object ThresholdCaller {
  def apply(variants: RDD[((String, Long), (Allele, Nucleotide))], threshold: Double = 0.2): RDD[VariantContext] = {
    variants
      .map(variant => (variant._2._2, variant._2._1))
      .groupByKey()
      .map {
        case (reference, alleles) => {
          val variant = alleles.foldLeft(0, 0, "") {
            case ((count, alleleCount, alleleVals), allele) => (count + 1,
                if (allele.value != reference.value) alleleCount + 1 else alleleCount,
                if (allele.value != reference.value && !alleleVals.contains(allele.value))
                  alleleVals + allele.value else alleleVals)
          }

          CalledVariant(reference.pos, reference.value, variant._3, ".", 0, variant._2, variant._1)
        }
      }
      .filter(variant => variant.alleleCount.toDouble / variant.count.toDouble >= threshold)
      .flatMap(variant => variant.alleles
                                  .split("")
                                  .map(Variant.newBuilder()
                                              .setReferenceAllele(variant.reference.toString)
                                              .setAlternateAllele(_)
                                              .setContigName(variant.pos._1)
                                              .setStart(variant.pos._2)
                                              .setEnd(variant.pos._2 + 1)
                                              .build()))
      .map(variant => VariantContext(variant))
  }
}

case class CalledVariant(pos: (String, Long),
                         reference: Char,
                         alleles: String,
                         id: String,
                         quality: Int,
                         alleleCount: Int,
                         count: Int) extends Serializable {
}
