package pl.edu.pw.elka.mbi.core.reads

import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.AlignmentRecord

object BaseObservations {
  //def SNPs = variants(_, _).filter(_ != Nil)

  def apply(rdd: RDD[AlignmentRecord], reference: RDD[((String, Long), Nucleotide)]) = {
    //rdd.flatMap(observationsFromRead(_, reference))
    val observations = rdd.flatMap(observationsFromRead).map(obs => ((obs.refName, obs.pos), obs))
    val comparison = observations.join(reference)
//    comparison.map {
//      case ((name, pos), (obs, ref)) =>
//    }
    comparison
  }
  private def observationsFromRead(read: AlignmentRecord) = {
    val alignments = parseCigar(read.getCigar)
    val sequence = read.getSequence

    alignments.foldLeft(IndexedSeq[BaseObservation](), 0) {
      case ((obs, index), alignment) => (obs ++ (alignment._1 match {
        case 'M' => sequence.substring(index, alignment._2)
                            .map(BaseObservation(read.getContigName,
                                                 read.getStart + index,
                                                 ".",
                                                 _,
                                                 read.getMapq))

        case _ => IndexedSeq[BaseObservation]()
      }), alignment._2)
    }._1
  }

/**
  private def observationsFromRead(read: AlignmentRecord, referenceFile: ReferenceFile) = {
    val alignments = parseCigar(read.getCigar)
    val sequence = read.getSequence
    val reference = referenceFile.extract(ReferenceRegion(read.getContigName, read.getStart, read.getEnd))

    alignments.foldLeft(IndexedSeq[BaseObservation](), 0) {
      case ((obs, index), alignment) => (obs ++ (alignment._1 match {
        case 'M' => for(seqMatch <- sequence.substring(index, alignment._2) zip
                                    reference.substring(index, alignment._2)) yield
                                    new BaseObservation(read.getContigName,
                                                        read.getStart + index,
                                                        ".",
                                                        seqMatch._1.toString,
                                                        seqMatch._2,
                                                        read.getMapq)

        case _ => IndexedSeq[BaseObservation]()
      }), alignment._2)
    }._1
  }
  */

  private def parseCigar(cigar: String) = {
    cigar
      .split("(?<=\\D)")
      .map(part => (part(part.length - 1), part.substring(0, part.length - 1).toInt))
  }
}

case class BaseObservation(refName: String,
                           pos: Long,
                           id: String = ".",
                           allele: Char,
                           quality: Int) extends Serializable {

}

