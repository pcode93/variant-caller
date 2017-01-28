package pl.edu.pw.elka.mbi.core.reads

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferencePosition
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.formats.avro.AlignmentRecord
import pl.edu.pw.elka.mbi.core.instrumentation.Timers._
import pl.edu.pw.elka.mbi.core.model.{Allele, AlignedAllele}
import pl.edu.pw.elka.mbi.core.util.Util

object VariantDiscovery {

  /**
    * Gets alleles from reads.
    * Joins alleles from reads with alleles from reference.
    * Groups alleles by the positions that they map to.
    * @param reads
    * @param reference
    * @return RDD of Alleles grouped by positions that they map to.
    */
  def apply(reads: RDD[AlignmentRecord],
            reference: RDD[(ReferencePosition, Allele)]): RDD[(ReferencePosition, Iterable[Allele])] = ReadAlleles.time {

    (reference ++
      reads
        .flatMap(mappedAllelesFromRead)
        .map(allele => (allele.pos, allele))
      ).groupByKey()
  }

  private def mappedAllelesFromRead(read: AlignmentRecord) = {
    val rich = RichAlignmentRecord(read)
    val alignments = Util.parseCigar(read.getCigar)
    val sequence = read.getSequence

    alignments.foldLeft(IndexedSeq[AlignedAllele](), (0,0)) {
      case ((obs, index), alignment) => alignment._1 match {
        case 'M' | 'X' | '=' => (obs ++ sequence
                            .substring(index._1, index._1 + alignment._2)
                            .zipWithIndex
                            .map {
                              case (allele, i) => AlignedAllele(ReferencePosition(read.getContigName,
                                                                read.getStart + index._2 + i),
                                                                ".",
                                                                allele.toString.toUpperCase,
                                                                read.getMapq)
                            }, (index._1 + alignment._2, index._2 + alignment._2))

        case 'D' | 'N' => (obs, (index._1, index._2 + alignment._2))
        case 'I' | 'S' => (obs, (index._1 + alignment._2, index._2))
        case _ => (obs, index)
      }
    }._1
  }
}



