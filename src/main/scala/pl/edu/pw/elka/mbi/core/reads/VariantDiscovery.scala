package pl.edu.pw.elka.mbi.core.reads

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferencePosition
import org.bdgenomics.formats.avro.AlignmentRecord
import pl.edu.pw.elka.mbi.core.instrumentation.Timers._
import pl.edu.pw.elka.mbi.core.model.{AlignedAllele, Allele}
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
    val alignments = Util.parseCigar(read.getCigar)
    val sequence = read.getSequence

    alignments.foldLeft(IndexedSeq[AlignedAllele](), (0, 0)) {
      case ((alleles, (readIndex, refIndex)), alignment) => alignment._1 match {
        case 'M' | 'X' | '=' => (
          alleles ++ 
            sequence
              .substring(readIndex, readIndex + alignment._2)
              .zipWithIndex
              .map {
                case (allele, i) => AlignedAllele(
                  ReferencePosition(
                    read.getContigName,
                    read.getStart + refIndex + i
                  ),
                  ".",
                  allele.toString.toUpperCase,
                  read.getMapq
                )
              },
          (readIndex + alignment._2, refIndex + alignment._2)
          )

        case 'D' | 'N' => (alleles, (readIndex, refIndex + alignment._2))
        case 'I' | 'S' => (alleles, (readIndex + alignment._2, refIndex))
        case _ => (alleles, (readIndex, refIndex))
      }
    }._1
  }
}



