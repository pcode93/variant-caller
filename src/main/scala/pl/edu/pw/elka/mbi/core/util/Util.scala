package pl.edu.pw.elka.mbi.core.util

object Util {

  /**
    * Parses a given CIGAR String
    * For example, "34M24D" would return
    * ((M, 34), (D, 24))
    * @param cigar CIGAR String
    * @return A sequence of (Operation, Length)
    */
  def parseCigar(cigar: String) = {
    cigar
      .split("(?<=\\D)")
      .map(part => {
        val substr = part.substring(0, part.length - 1)
        (part(part.length - 1), if(substr != "") substr.toInt else -1)
      })
      .filter(_._2 != -1)
  }
}
