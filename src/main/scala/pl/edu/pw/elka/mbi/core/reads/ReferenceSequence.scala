package pl.edu.pw.elka.mbi.core.reads

import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.NucleotideContigFragment
import pl.edu.pw.elka.mbi.core.instrumentation.Timers._
import pl.edu.pw.elka.mbi.core.model.ReferenceAllele

object ReferenceSequence {
  /**
    *
    * @param reference Reference sequence.
    * @return RDD of ReferenceAlleles mapped to their positions in the sequence.
    */
  def apply(reference: RDD[NucleotideContigFragment]): RDD[((String, Long), ReferenceAllele)] = ReferenceAlleles.time {
    reference.flatMap(fragment => {
      val name = fragment.getContig.getContigName
      var pos = fragment.getFragmentStartPosition

      fragment.getFragmentSequence.map(alleleVal => {
        val allele = ReferenceAllele((name, pos), alleleVal.toString)
        pos += 1
        allele
      })
    }).map(n => (n.pos, n))
  }
}



