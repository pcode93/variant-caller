package pl.edu.pw.elka.mbi.core.model

import org.bdgenomics.adam.models.ReferencePosition

case class ReferenceAllele(pos: ReferencePosition, value: String) extends Allele(value) {

}
