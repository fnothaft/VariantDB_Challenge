/**
 *
 */
val dataDir = "/data/variant_db/1"
val relatednessFile = "README.sample_cryptic_relations"
val populationsFile = "phase1_integrated_calls.20101123.ALL.panel"
val vcfFile = "1KG.chr22.anno.infocol.vcf"

import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.rdd.MetricsContext._
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.GenotypeAllele
import org.bdgenomics.utils.instrumentation.Metrics
import scala.io.Source

object Timers extends Metrics {

  // timers for setup code
  val mungeVcf = timer("Munging whitespace in VCF")
  val convertVcfToParquet = timer("Converting VCF to Parquet")
  val loadMetadata = timer("Loading sample metadata")

  // timers for running the query in pure spark (rdd's)
  val pureSparkIntegrate = timer("Running integration query in pure Spark")
  val pureSparkWithCachingIntegrate = timer("Running integration query in pure Spark with cached input")
  val broadcastMetadata = timer("Broadcasting metadata")
  val cacheInputRdd = timer("Loading and caching input RDD")
  val queryRdd = timer("Querying the RDD")
}

// pull in all the timers
import Timers._

// convert VCF into ADAM genotypes and save to disk
convertVcfToParquet.time {
  sc.loadGenotypes("%s/%s".format(dataDir, vcfFile))
    .saveAsParquet("%s/chr22.annotated.gt.adam".format(dataDir))
}

case class Sample(sampleId: String, population: String)

// parse the sample metadata
val (samples, relations) = loadMetadata.time {
  
  // make hadoop paths for the metadata files
  val relationsPath = new Path("%s/%s".format(dataDir, relatednessFile))
  val samplePath = new Path("%s/%s".format(dataDir, populationsFile))

  // get the filesystem for the sample metadata
  val fs = relationsPath.getFileSystem(sc.hadoopConfiguration)

  // open the two files
  val relationsStream = fs.open(relationsPath)
  val sampleStream = fs.open(relationsPath)

  // extract the cryptic relatedness data
  val parsedRelations = Source.fromInputStream(relationsStream)
    .getLines
    .drop(4) // first 4 lines are whitespace
    .flatMap(line => {
      // line is tab delimited:
      // Population      Sample 1        Sample 2        Relationship    IBD0    IBD1    IBD2
      val splitLine = line.split("\t")

      if (splitLine.length != 7) {
        Iterable.empty[Sample]
      } else {
        val population = splitLine(0)

	// exclude samples in ASW that are siblings
	// we will not filter them out
	if (population == "ASW" &&
	    splitLine(3).startsWith("Sibling")) {
	  Iterable.empty[Sample]
	} else {
	  Iterable(Sample(splitLine(1), population),
	    Sample(splitLine(2), population))
	}
      }
    }).toSeq

  // extract the population labels
  val parsedPopulations = Source.fromInputStream(sampleStream)
    .getLines
    .flatMap(line => {
      // line is tab delimited:
      // SampleId Population ? SequencerPlatform
      val splitLine = line.split("\t")
      
      if (splitLine.length > 2) {
        Some(Sample(splitLine(0), splitLine(1)))
      } else {
        None
      }
    }).toSeq

  // Scala.io.source is lazy --> force materialization
  println("Have %d related samples across %d populations.".format(parsedRelations.size,
    parsedPopulations.size))

  (parsedPopulations, parsedRelations)
}

// load the rdd from disk
val genotypes = sc.loadParquetGenotypes("%s/chr22.annotated.gt.adam".format(dataDir))

// munge and broadcast the sample metadata
val bcastSampleToPopulationMap = broadcastMetadata.time {
  val relatedSamples = relations.map(_.sampleId).toSet
  val sampleToPopulationMap = samples.filter(sample => relatedSamples(sample.sampleId))
    .map(sample => (sample.sampleId, sample.population))
    .toArray
    .toMap
 sc.broadcast(sampleToPopulationMap)
}

// are we caching the data? if so, cache it now
val cachedGenotypes = cacheInputRdd.time {
    
  // cache the genotypes and force the genotypes to be materialized into
  // memory by doing a count
  val cachedGenotypes = genotypes.transform(_.cache)
  println("Have %d genotypes.".format(cachedGenotypes.rdd.count))

  cachedGenotypes
}

val variantsByPopulation = queryRdd.time {
 cachedGenotypes.rdd
  .flatMap(gt => {
      bcastSampleToPopulationMap.value
       .get(gt.getSampleId)
       .map(population => (population, gt))
    }).filter(kv => {
      val (_, gt) = kv
        ((gt.getAlleles.contains(GenotypeAllele.REF) &&
	  gt.getAlleles.contains(GenotypeAllele.ALT)) && // is het
	 (Option(gt.getReadDepth).map(i => i: Int).orElse({
	   (Option(gt.getReferenceReadDepth), Option(gt.getAlternateReadDepth)) match {
	     case (Some(refDepth), Some(altDepth)) => Some(refDepth + altDepth)
	     case _ => None
	   }
	 }).fold(true)(_ > 30)) && // DP or sum(AD) is > 30, if provided
	 Option(gt.getAlternateReadDepth).fold(false)(_ > 10) && // alt depth > 10
	 Option(gt.getVariant.getAnnotation.getAttributes.get("SAVANT_IMPACT")).filter(impact => {
	   impact == "HIGH" || impact == "MEDIUM"
	 }).isDefined &&
	 Option(gt.getVariant.getAnnotation.getAttributes.get("ExAC.Info.AF")).fold(true)(afString => {
	   try {
	     afString.toFloat < 0.1
	  } catch {
	    case t: Throwable => {
	      true
	    }
	  }
	 }))
      }).map(kv => {
       val (population, gt) = kv
       (ReferenceRegion(gt), population, gt.getVariant.getAlternateAllele)
      }).distinct
        .map(t => {
	  val (_, population, _) = t
      }).countByValue
    }
  }

println(variantsByPopulation.mkString("\n"))