package pl.edu.pw.elka.mbi.core.util

import org.scalatest.FlatSpec

class UtilSpec extends FlatSpec {

  "Correctly parsed CIGAR String" should "return an array of tuples: (Operator, length)" in {
    assert(Util.parseCigar("1M1I") sameElements Array(('M', 1), ('I', 1)))
  }

  "0 length CIGAR operators" should "get filtered out" in {
    assert(Util.parseCigar("1D2MI").count(_._2 == 0) == 0)
  }
}
