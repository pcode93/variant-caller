package pl.edu.pw.elka.mbi.core.reads

import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.NucleotideContigFragment

object Reference {
  def apply(rdd: RDD[NucleotideContigFragment]): RDD[((String, Long), Nucleotide)] = {
    rdd.flatMap(fragment => {
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

case class Nucleotide(pos: (String, Long), value: Char) extends Serializable {

}

