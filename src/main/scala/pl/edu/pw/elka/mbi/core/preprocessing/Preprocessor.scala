package pl.edu.pw.elka.mbi.core.preprocessing

import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD

class Preprocessor(alignment: AlignmentRecordRDD) {
  def realignIndels() = {
    alignment.realignIndels()
    this
  }

  def filterByBaseQuality(threshold: Int) = {
    this
  }

  def filterByMappingQuality(threshold: Int) = {
    this
  }

  def reads = alignment.rdd
}

