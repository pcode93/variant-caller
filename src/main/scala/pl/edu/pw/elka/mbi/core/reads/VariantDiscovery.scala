package pl.edu.pw.elka.mbi.core.reads

import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.AlignmentRecord
import pl.edu.pw.elka.mbi.cli.VariantCaller
import pl.edu.pw.elka.mbi.core.Timers._

object VariantDiscovery {

  def apply(reads: RDD[AlignmentRecord], reference: RDD[((String, Long), Nucleotide)]) = {
    VariantCaller.debug("----------------READS---------------------", reads.map(_.toString))

    val observations = ReadAlleles.time {
      reads.flatMap(variantsFromRead).map(v => ((v.refName, v.pos), v))
    }

    VariantCaller.debug("----------------VARIANTS---------------------", observations.map(_.toString))

    JoinReadsWithReferences.time {
      observations.join(reference)
    }
  }

  private def variantsFromRead(read: AlignmentRecord) = {
    val alignments = parseCigar(read.getCigar)
    val sequence = read.getSequence

    alignments.foldLeft(IndexedSeq[Allele](), 0) {
      case ((obs, index), alignment) => alignment._1 match {
        case 'M' => (obs ++ sequence
                            .substring(index, index + alignment._2)
                            .zipWithIndex
                            .map {
                              case (allele, i) => Allele(read.getContigName,
                                                         read.getStart + index + i,
                                                         ".",
                                                         allele,
                                                         read.getMapq)
                            }, index + alignment._2)

        case 'D' => (IndexedSeq[Allele](), index)
        case 'I' => (IndexedSeq[Allele](), index + alignment._2)
        case _ => (IndexedSeq[Allele](), index)
      }
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

