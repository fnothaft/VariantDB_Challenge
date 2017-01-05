/**
 *
 */
val dataDir = "/data/variant_db/2"
//val na12878 = "NA12878.chr22.g.vcf.gz"
//val na12891 = "NA12891.chr22.g.vcf.gz"
//val na12892 = "NA12892.chr22.g.vcf.gz"
val na12878 = "NA12878.chr22.g.vcf"
val na12891 = "NA12891.chr22.g.vcf"
val na12892 = "NA12892.chr22.g.vcf"

import htsjdk.samtools.ValidationStringency
import org.apache.spark.rdd.MetricsContext._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variant.VariantRDD
import org.bdgenomics.formats.avro.GenotypeAllele
import scala.collection.JavaConversions._

// convert data into parquet
val partitions = 64
//sc.loadGenotypes("%s/%s".format(dataDir, na12878)).transform(_.repartition(partitions)).saveAsParquet("%s/NA12878.gt.adam".format(dataDir))
//sc.loadGenotypes("%s/%s".format(dataDir, na12891)).transform(_.repartition(partitions)).saveAsParquet("%s/NA12891.gt.adam".format(dataDir))
//sc.loadGenotypes("%s/%s".format(dataDir, na12892)).transform(_.repartition(partitions)).saveAsParquet("%s/NA12892.gt.adam".format(dataDir))
sc.loadGenotypes("%s/%s".format(dataDir, na12878)).saveAsParquet("%s/NA12878.gt.adam".format(dataDir))
sc.loadGenotypes("%s/%s".format(dataDir, na12891)).saveAsParquet("%s/NA12891.gt.adam".format(dataDir))
sc.loadGenotypes("%s/%s".format(dataDir, na12892)).saveAsParquet("%s/NA12892.gt.adam".format(dataDir))

// reload datasets and cache
val na12878Gts = sc.loadGenotypes("%s/NA12878.gt.adam".format(dataDir)).transform(_.cache())
val otherGts = sc.loadGenotypes("%s/NA1289*.gt.adam/*".format(dataDir)).transform(_.cache())

// force materialization
println("NA12878 has %d genotypes.".format(na12878Gts.rdd.count))
println("NA12891/2 has %d genotypes.".format(otherGts.rdd.count))

// filter high quality genotypes, hom alt in 12878, hom ref in others
val gqThreshold = 20
val na12878HomAltGts = na12878Gts.transform(rdd => {
  rdd.filter(gt => {
    val gtCalls = gt.getAlleles
    Option(gt.getGenotypeQuality).fold(false)(_ >= gqThreshold) &&
    !gtCalls.isEmpty &&
    gtCalls.forall(_ == GenotypeAllele.ALT)
  })
})
val otherHomRefGts = otherGts.transform(rdd => {
  rdd.filter(gt => {
    val gtCalls = gt.getAlleles
    !gtCalls.isEmpty &&
    gtCalls.forall(_ == GenotypeAllele.REF) &&
    Option(gt.getGenotypeQuality).fold(false)(gq => {
      if (gq == 0) {
        // this is fun code.
	// 
	// the tool that generated the gVCFs we are working with occasionally writes out
	// GQ = 0 for Hom REF gVCF lines where the Ref/Ref PL entry is high confidence.
	//
	// what is likely happening here can be inferred from one of the suspect lines,
	// from the NA12892 VCF:
	// chr22   24300634        .       G       <NON_REF>       .       .       END=24300634    GT:DP:GQ:MIN_DP:PL      0/0:24:0:24:0,0,713
	//
	// if you look at the PL field, Ref/Ref PL is very high while Alt/Alt and Alt/Ref PLs are 0.
	// probably what is happening is that when the tool is calculating the GQ from PLs, it's doing something like
	// max(PL) / sum(PL), or alternatively max(PL) / sum g in 0..m,g!=argmax_idx(PL(idx)) PL(g)
	// and dumping a bad GQ
	//
	// since these lines are obviously strong HomRef calls, let's recover them by parsing PL's
        if (gt.getVariant.getAlternateAllele == null) {
          val nonReferenceLikelihoods = gt.getNonReferenceLikelihoods.toSeq
	  nonReferenceLikelihoods.dropRight(1).forall(_ == java.lang.Double.NEGATIVE_INFINITY) &&
	    nonReferenceLikelihoods.last != java.lang.Double.NEGATIVE_INFINITY
	} else {
	  false
	}
      } else {
        gq >= gqThreshold
      }
    })
  })
})

// now, do a broadcast region join
val joinedGts = na12878HomAltGts.broadcastRegionJoin(otherHomRefGts)

// and then take the variants that show up in two samples
//
// in a simple world, we would just count the number of home ref genotypes
// that overlapped a given hom alt site. howaaaaaayver, a poorly formatted
// gVCF "could" contain multiple overlapping records at a single site.
// this would occur if we scored two different alts, and emitted hom ref
// calls against each alt.
//
// note: this happens on the test data
val selectedVariants = joinedGts.rdd.map(p => {
    val (na12878Gt, gt) = p
    (na12878Gt.getVariant, Set(gt.getSampleId))
  }).reduceByKey(_ ++ _).filter(kv => kv._2.size == 2).keys

// and save as vcf
VariantRDD(selectedVariants,
  na12878Gts.sequences).saveAsVcf("%s/selectedVariants.vcf".format(dataDir),
    asSingleFile = true, stringency = ValidationStringency.LENIENT)