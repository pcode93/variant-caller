package pl.edu.pw.elka.mbi.core.model

case class Allele(refName: String,
                  pos: Long,
                  id: String = ".",
                  value: Char,
                  quality: Int) extends Serializable {

}
