package pl.edu.pw.elka.mbi.core.variants

import org.apache.spark.rdd.RDD
import pl.edu.pw.elka.mbi.core.reads.{Nucleotide, Allele}

object ThresholdCaller {
  def apply(variants: RDD[((String, Long), (Allele, Nucleotide))], threshold: Double = 0.2) = {
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
