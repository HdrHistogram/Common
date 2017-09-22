package org.hdrhistogram.tools

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import org.HdrHistogram.Histogram
import org.HdrHistogram.HistogramIterationValue
import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Paths
import java.util.zip.GZIPOutputStream

object MetadataTool {
    @JvmStatic
    fun main(args: Array<String>) {
        // provide directory containing histograms
        val dir = File(args[0])

        val writer = ObjectMapper().writer()

        Files.newDirectoryStream(dir.toPath(), "*.histo").use { stream ->
            stream.forEach { path ->

                val buf = ByteBuffer.wrap(Files.readAllBytes(path))
                val histo = Histogram.decodeFromCompressedByteBuffer(buf, 1)

                val metadata = HistogramMetadata(count = histo.totalCount,
                        max = histo.maxValue,
                        min = histo.minValue,
                        lowestDiscernibleValue = histo.lowestDiscernibleValue,
                        highestTrackableValue = histo.highestTrackableValue,
                        significantValueDigits = histo.numberOfSignificantValueDigits,
                        mean = histo.mean,
                        stdDev = histo.stdDeviation,
                        valuesAtPercentiles = (0..1000)
                                // sample every 10th of a percent
                                .map { it.toDouble() / 10 }
                                // turn it into a map of percentile to value
                                .associateBy({ it }, { i -> histo.getValueAtPercentile(i) }),
                        iterators = Iterators(
                                linear = (0..14)
                                        // linear stride across several powers of 2
                                        .map { 1L.shl(it) }
                                        .associateBy({ "$it" },
                                                { histo.linearBucketValues(it).map { IteratorValue(it) } }),
                                percentile = (0..9)
                                        .map { 1.shl(it) }
                                        .associateBy({ "$it" }, { histo.percentiles(it).map { IteratorValue(it) } }),
                                logarithmic = listOf(1L, 100, 1000)
                                        // combine different first bucket and log bases into "firstbucket-logbase" names
                                        .flatMap { firstBucketSize ->
                                            listOf(1.1, 2.0, 10.0).map { logBase ->
                                                Pair(firstBucketSize, logBase)
                                            }
                                        }
                                        .associateBy({ "${it.first}-${it.second}" }, {
                                            histo.logarithmicBucketValues(it.first, it.second).map { IteratorValue(it) }
                                        }),
                                recorded = histo.recordedValues().map { IteratorValue(it) },
                                all = histo.allValues().map { IteratorValue(it) })
                )

                val metadataFileName = path.fileName.toString().replace("\\.histo$", "-metadata.json.gz")
                Files.newOutputStream(Paths.get(metadataFileName)).use { fos ->
                    GZIPOutputStream(fos).use { gzos ->
                        writer.writeValue(gzos, metadata)
                    }
                }
            }
        }
    }
}

class HistogramMetadata(@JsonProperty("totalCount") val count: Long,
                        @JsonProperty("maxValue") val max: Long,
                        @JsonProperty("minValue") val min: Long,
                        @JsonProperty("lowestDiscernibleValue") val lowestDiscernibleValue: Long,
                        @JsonProperty("highestTrackableValue") val highestTrackableValue: Long,
                        @JsonProperty("significantValueDigits") val significantValueDigits: Int,
                        @JsonProperty("mean") val mean: Double,
                        @JsonProperty("stdDev") val stdDev: Double,
                        @JsonProperty("valuesAtPercentiles") val valuesAtPercentiles: Map<Double, Long>,
                        @JsonProperty("iterators") val iterators: Iterators
)

class IteratorValue(@JsonProperty("valueIteratedTo") val valueIteratedTo: Long,
                    @JsonProperty("countAddedInThisIterationStep") val countAddedInThisIterationStep: Long,
                    @JsonProperty("countAtValueIteratedTo") val countAtValueIteratedTo: Long,
                    @JsonProperty("percentile") val percentile: Double,
                    @JsonProperty("percentileLevelIteratedTo") val percentileLevelIteratedTo: Double,
                    @JsonProperty("totalCountToThisValue") val totalCountToThisValue: Long) {
    constructor(v: HistogramIterationValue) : this(v.valueIteratedTo,
            v.countAddedInThisIterationStep,
            v.countAtValueIteratedTo,
            v.percentile,
            v.percentileLevelIteratedTo,
            v.totalCountToThisValue
    )
}

class Iterators(@JsonProperty("linear") val linear: Map<String, List<IteratorValue>>,
                @JsonProperty("percentile") val percentile: Map<String, List<IteratorValue>>,
                @JsonProperty("logarithmic") val logarithmic: Map<String, List<IteratorValue>>,
                @JsonProperty("recorded") val recorded: List<IteratorValue>,
                @JsonProperty("all") val all: List<IteratorValue>
)
