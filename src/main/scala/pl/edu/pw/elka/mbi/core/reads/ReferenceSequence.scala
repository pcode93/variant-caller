package pl.edu.pw.elka.mbi.core.reads

import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.NucleotideContigFragment
import pl.edu.pw.elka.mbi.core.instrumentation.Timers._
import pl.edu.pw.elka.mbi.core.model.Nucleotide

object ReferenceSequence {
  def apply(reference: RDD[NucleotideContigFragment]): RDD[((String, Long), Nucleotide)] = ReferenceAlleles.time {
    reference.flatMap(fragment => {
      val name = fragment.getContig.getContigName
      var pos = fragment.getFragmentStartPosition

      fragment.getFragmentSequence.map(nucleotideVal => {
        val nucleotide = Nucleotide((name, pos), nucleotideVal)
        pos += 1
        nucleotide
      })
    }).map(n => (n.pos, n))
  }
}



