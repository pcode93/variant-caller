package pl.edu.pw.elka.mbi.core.util

object Util {

  def parseCigar(cigar: String) = {
    cigar
      .split("(?<=\\D)")
      .map(part => (part(part.length - 1), part.substring(0, part.length - 1).toInt))
  }
}
