package pl.edu.pw.elka.mbi.core.model

import org.bdgenomics.adam.models.ReferencePosition

case class AlignedAllele(pos: ReferencePosition,
                         id: String = ".",
                         value: String,
                         quality: Int) extends Allele(value){

}
