package pl.edu.pw.elka.mbi.core

import org.bdgenomics.utils.instrumentation.Metrics

object Timers extends Metrics {
  val LoadReference = timer("LoadReference")
  val LoadReads = timer("LoadReads")
  val ReferenceAlleles = timer("ReferenceAlleles")
  val ReadAlleles = timer("ReadAlleles")
  val JoinReadsWithReferences = timer("JoinReadsWithReferences")
  val CallVariants = timer("CallVariants")
}

