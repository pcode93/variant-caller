package pl.edu.pw.elka.mbi.core.model

case class AlignedAllele(refName: String,
                         pos: Long,
                         id: String = ".",
                         value: String,
                         quality: Int) extends Serializable {

}
