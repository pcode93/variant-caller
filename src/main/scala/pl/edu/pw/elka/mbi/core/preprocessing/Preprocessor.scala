package pl.edu.pw.elka.mbi.core.preprocessing

import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD

class Preprocessor(alignment: AlignmentRecordRDD) {
  /**
    * Realigns Indels
    * @return this
    */
  def realignIndels() = {
    alignment.realignIndels()
    this
  }

  /**
    * Filters alignment records by their base quality.
    * Records with base quality below the threshold get filtered out.
    * @param threshold Minimum base quality value.
    * @return this
    */
  def filterByBaseQuality(threshold: Int) = {
    this
  }

  /**
    * Filters alignment records by their mapping quality.
    * Records with mapping quality below the threshold get filtered out.
    * @param threshold Minimum mapping quality value.
    * @return this
    */
  def filterByMappingQuality(threshold: Int) = {
    this
  }

  /**
    *
    * @return RDD of AlignmentRecords.
    */
  def reads = alignment.rdd
}

