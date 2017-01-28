package pl.edu.pw.elka.mbi.core.reads

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferencePosition
import org.bdgenomics.formats.avro.NucleotideContigFragment
import pl.edu.pw.elka.mbi.core.instrumentation.Timers._
import pl.edu.pw.elka.mbi.core.model.{Allele, ReferenceAllele}

object ReferenceSequence {
  /**
    * Gets alleles from the reference sequence.
    * @param reference Reference sequence.
    * @return RDD of ReferenceAlleles mapped to their positions in the sequence.
    */
  def apply(reference: RDD[NucleotideContigFragment]): RDD[(ReferencePosition, Allele)] = ReferenceAlleles.time {
    reference.flatMap(fragment => {
      val name = fragment.getContig.getContigName
      var pos = fragment.getFragmentStartPosition

      fragment.getFragmentSequence.map(alleleVal => {
        val allele = ReferenceAllele(ReferencePosition(name, pos), alleleVal.toString.toUpperCase)
        pos += 1
        allele
      })
    }).map(n => (n.pos, n))
  }
}



