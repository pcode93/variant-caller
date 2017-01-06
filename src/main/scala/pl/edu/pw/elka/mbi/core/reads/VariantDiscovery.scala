package pl.edu.pw.elka.mbi.core.reads

import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.AlignmentRecord
import pl.edu.pw.elka.mbi.cli.VariantCaller
import pl.edu.pw.elka.mbi.core.instrumentation.Timers._
import pl.edu.pw.elka.mbi.core.model.{Allele, Nucleotide}
import pl.edu.pw.elka.mbi.core.util.Util

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
    val alignments = Util.parseCigar(read.getCigar)
    val sequence = read.getSequence

    alignments.foldLeft(IndexedSeq[Allele](), (0,0)) {
      case ((obs, index), alignment) => alignment._1 match {
        case 'M' => (obs ++ sequence
                            .substring(index._1, index._1 + alignment._2)
                            .zipWithIndex
                            .map {
                              case (allele, i) => Allele(read.getContigName,
                                                         read.getStart + index._2 + i,
                                                         ".",
                                                         allele,
                                                         read.getMapq)
                            }, (index._1 + alignment._2, index._2 + alignment._2))

        case 'D' => (obs, (index._1, index._2 + alignment._2))
        case 'I' => (obs, (index._1 + alignment._2, index._2 + alignment._2))
        case _ => (obs, index)
      }
    }._1
  }
}



