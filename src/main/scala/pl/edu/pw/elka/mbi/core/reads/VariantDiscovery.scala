package pl.edu.pw.elka.mbi.core.reads

import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.AlignmentRecord
import pl.edu.pw.elka.mbi.cli.VariantCaller
import pl.edu.pw.elka.mbi.core.instrumentation.Timers._
import pl.edu.pw.elka.mbi.core.model.{AlignedAllele, ReferenceAllele}
import pl.edu.pw.elka.mbi.core.util.Util

object VariantDiscovery {

  /**
    * Gets variants from AlignmentRecords maps them to their positions in the sequence.
    * Then joins those variants with the @reference,
    * resulting in a RDD of variants and ReferenceAlleles mapped to the same positions.
    * @param reads RDD of AlignmentRecords.
    * @param reference RDD of ReferenceAlleles mapped to their positions in the sequence.
    * @return RDD of variants and ReferenceAlleles mapped to their positions in the sequence.
    */
  def apply(reads: RDD[AlignmentRecord],
            reference: RDD[((String, Long), ReferenceAllele)]): RDD[((String, Long), (AlignedAllele, ReferenceAllele))] = {

    val alleles = ReadAlleles.time {
      reads.flatMap(mappedAllelesFromRead).map(allele => ((allele.refName, allele.pos), allele))
    }

    VariantCaller.debug("Read alleles from alignment records")

    JoinReadsWithReferences.time {
      alleles.join(reference)
    }
  }

  private def mappedAllelesFromRead(read: AlignmentRecord) = {
    val alignments = Util.parseCigar(read.getCigar)
    val sequence = read.getSequence

    alignments.foldLeft(IndexedSeq[AlignedAllele](), (0,0)) {
      case ((obs, index), alignment) => alignment._1 match {
        case 'M' => (obs ++ sequence
                            .substring(index._1, index._1 + alignment._2)
                            .zipWithIndex
                            .map {
                              case (allele, i) => AlignedAllele(read.getContigName,
                                                                read.getStart + index._2 + i,
                                                                ".",
                                                                allele.toString,
                                                                read.getMapq)
                            }, (index._1 + alignment._2, index._2 + alignment._2))

        case 'D' => (obs, (index._1, index._2 + alignment._2))
        case 'I' => (obs, (index._1 + alignment._2, index._2 + alignment._2))
        case _ => (obs, index)
      }
    }._1
  }
}



