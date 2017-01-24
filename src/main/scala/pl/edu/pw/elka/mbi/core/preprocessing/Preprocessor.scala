package pl.edu.pw.elka.mbi.core.preprocessing

import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD

class Preprocessor(alignment: AlignmentRecordRDD) {

  private var reads = alignment.rdd
  /**
    * Realigns Indels
    * @return this
    */
  def realignIndels() = {
    alignment.realignIndels()
    this
  }

  /**
    * Filters alignment records by their mapping quality.
    * Records with mapping quality below the threshold get filtered out.
    * @param threshold Minimum mapping quality value.
    * @return this
    */
  def filterByMappingQuality(threshold: Int) = {
    reads = reads.filter(_.getMapq >= threshold)
    this
  }

  /**
    *
    * @return RDD of AlignmentRecords.
    */
  def getReads = reads
}

