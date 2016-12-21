package pl.edu.pw.elka.mbi.core.reads

import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.AlignmentRecord

object VariantDiscovery {

  def apply(reads: RDD[AlignmentRecord], reference: RDD[((String, Long), Nucleotide)]) = {
    val observations = reads.flatMap(variantsFromRead).map(v => ((v.refName, v.pos), v))
    val comparison = observations.join(reference)
//    comparison.map {
//      case ((name, pos), (variant, ref)) =>
//    }
    comparison
  }

  private def variantsFromRead(read: AlignmentRecord) = {
    val alignments = parseCigar(read.getCigar)
    val sequence = read.getSequence

    alignments.foldLeft(IndexedSeq[Allele](), 0) {
      case ((obs, index), alignment) => (obs ++ (alignment._1 match {
        case 'M' => sequence.substring(index, alignment._2)
                            .map(Allele(read.getContigName,
                                        read.getStart + index,
                                        ".",
                                        _,
                                        read.getMapq))

        case _ => IndexedSeq[Allele]()
      }), alignment._2)
    }._1
  }

  private def parseCigar(cigar: String) = {
    cigar
      .split("(?<=\\D)")
      .map(part => (part(part.length - 1), part.substring(0, part.length - 1).toInt))
  }
}

case class Allele(refName: String,
                           pos: Long,
                           id: String = ".",
                           value: Char,
                           quality: Int) extends Serializable {

}

