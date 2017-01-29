package pl.edu.pw.elka.mbi.core.reads

import org.bdgenomics.formats.avro.{Contig, NucleotideContigFragment}
import pl.edu.pw.elka.mbi.core.WithSparkContextSpec

class ReferenceSequenceSpec extends WithSparkContextSpec {

  it should "get ReferenceAlleles from the reference sequence" in {
    val contigNames = Array("1", "2", "3")
    val sequence = "ACGT"
    val expectedNumberOfAlleles = contigNames.length * sequence.length

    assert(
      ReferenceSequence(
        sc.parallelize(
          for(contigName <- Array("1", "2", "3"))
            yield {
              val contig = Contig
                .newBuilder()
                .setContigName(contigName)
                .build()

              NucleotideContigFragment
                .newBuilder()
                .setContig(contig)
                .setFragmentStartPosition(1L)
                .setFragmentSequence("ACGT")
                .build()
            }
        )
      ).count() == expectedNumberOfAlleles
    )
  }
}
