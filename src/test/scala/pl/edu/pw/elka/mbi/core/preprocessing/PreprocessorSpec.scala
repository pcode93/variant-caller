package pl.edu.pw.elka.mbi.core.preprocessing

import org.bdgenomics.adam.models.RecordGroupDictionary
import org.bdgenomics.adam.rdd.read.UnalignedReadRDD
import org.bdgenomics.formats.avro.AlignmentRecord
import pl.edu.pw.elka.mbi.core.WithSparkContextSpec

class PreprocessorSpec extends WithSparkContextSpec {

  "Reads with mapping quality below the threshold" should "get filtered out" in {
    assert(
      new Preprocessor(
        UnalignedReadRDD(
          sc.parallelize(
            for(i <- Array(1,2,3))
              yield AlignmentRecord
                      .newBuilder()
                      .setMapq(10)
                      .build()
          ),
          RecordGroupDictionary.empty
        )
      )
      .filterByMappingQuality(3)
      .getReads
      .map(_.getMapq)
      .min() >= 3
    )
  }
}
