package org.hdrhistogram.tools

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.guava.GuavaModule
import com.google.common.collect.Iterables.transform
import joptsimple.OptionException
import joptsimple.OptionParser
import joptsimple.OptionSet
import org.HdrHistogram.AbstractHistogram
import org.HdrHistogram.Histogram
import org.HdrHistogram.HistogramIterationValue
import org.HdrHistogram.HistogramLogReader
import org.tukaani.xz.LZMA2Options
import org.tukaani.xz.XZOutputStream
import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletionService
import java.util.concurrent.ExecutorCompletionService
import java.util.concurrent.Executors

object MetadataTool {
    @JvmStatic
    fun main(args: Array<String>) {
        val parser = OptionParser()
        val dirOpt = parser.accepts("dir", "Dir with histogram samples")
                .withRequiredArg().ofType(String::class.java).describedAs("directory").required()

        var opts: OptionSet? = null
        try {
            opts = parser.parse(*args)
        } catch (e: OptionException) {
            parser.printHelpOn(System.err)
            System.exit(1)
        }

        val start = Instant.now()

        val dir = File(opts!!.valueOf(dirOpt))

        val mapper = ObjectMapper()
        mapper.registerModule(GuavaModule())
        val jsonWriter = mapper.writerWithDefaultPrettyPrinter()

        // It's trivial to parallelize
        val pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors())
        val service: CompletionService<Path> = ExecutorCompletionService(pool)

        val futures = Files.newDirectoryStream(dir.toPath(), "*.histo").use { stream ->
            stream.map { path ->
                service.submit({
                    println("Starting $path")
                    val buf = ByteBuffer.wrap(Files.readAllBytes(path))
                    val histo = Histogram.decodeFromCompressedByteBuffer(buf, 1)

                    val testDataPath = replaceSuffix(path, Regex("\\.histo$"), "-metadata.json.xz")
                    Files.newOutputStream(testDataPath).use { fos ->
                        XZOutputStream(fos, LZMA2Options()).use { xzos ->
                            jsonWriter.writeValue(xzos, HistogramTestData(histo))
                        }
                    }

                    path
                })
            }.toMutableList()
        }

        futures.addAll(Files.newDirectoryStream(dir.toPath(), "*.hlog").use { stream ->
            stream.map { path ->
                service.submit({
                    println("Starting $path")
                    val reader = HistogramLogReader(path.toFile())

                    val histograms = mutableListOf<HistogramLogEntry>()

                    while (reader.hasNext()) {
                        val h = reader.nextIntervalHistogram() as AbstractHistogram

                        histograms.add(HistogramLogEntry(h))
                    }

                    val testDataPath = replaceSuffix(path, Regex("\\.hlog$"), "-metadata.json.xz")
                    Files.newOutputStream(testDataPath).use { fos ->
                        XZOutputStream(fos, LZMA2Options()).use { xzos ->
                            jsonWriter.writeValue(xzos, HistogramLogTestData(histograms))

                        }
                    }

                    path
                })
            }.toMutableList()
        })

        futures.forEach { f ->
            println("Finished ${f.get()}")
        }

        pool.shutdown()

        println("Elapsed time: ${Duration.between(start, Instant.now())}")
    }
}

private fun replaceSuffix(path: Path, suffixPattern: Regex, replacement: String): Path {
    val metadataFileName = path.fileName.toString().replace(suffixPattern, replacement)
    return path.parent.resolve(metadataFileName)
}


class HistogramLogEntry(@JsonProperty("tag") val tag: String?,
                        @JsonProperty("startTime") val startTime: Long,
                        @JsonProperty("endTime") val endTime: Long,
                        @JsonProperty("histogram") val histogram: HistogramTestData) {
    constructor(h: AbstractHistogram) : this(h.tag, h.startTimeStamp, h.endTimeStamp, HistogramTestData(h))
}

class HistogramLogTestData(@JsonProperty("entries") val entries: List<HistogramLogEntry>)

class HistogramTestData(@JsonProperty("totalCount") val count: Long,
                        @JsonProperty("maxValue") val max: Long,
                        @JsonProperty("minValue") val min: Long,
                        @JsonProperty("lowestDiscernibleValue") val lowestDiscernibleValue: Long,
                        @JsonProperty("highestTrackableValue") val highestTrackableValue: Long,
                        @JsonProperty("significantValueDigits") val significantValueDigits: Int,
                        @JsonProperty("mean") val mean: Double,
                        @JsonProperty("stdDev") val stdDev: Double,
                        @JsonProperty("valuesAtPercentiles") val valuesAtPercentiles: Map<Double, Long>,
                        @JsonProperty("iterators") val iterators: Iterators) {
    constructor(h: AbstractHistogram) : this(count = h.totalCount,
            max = h.maxValue,
            min = h.minValue,
            lowestDiscernibleValue = h.lowestDiscernibleValue,
            highestTrackableValue = h.highestTrackableValue,
            significantValueDigits = h.numberOfSignificantValueDigits,
            mean = h.mean,
            stdDev = h.stdDeviation,
            valuesAtPercentiles = (0..1000)
                    // sample every 10th of a percent
                    .map { it.toDouble() / 10 }
                    // turn it into a map of percentile to value
                    .associateBy({ it }, { i -> h.getValueAtPercentile(i) }),
            iterators = Iterators(
                    linear = (6..14)
                            // linear stride with strides at several powers of 2
                            .map { 1L.shl(it) }
                            .associateBy({ "$it" },
                                    { transform(h.linearBucketValues(it)) { IteratorValue(it!!) } }),
                    percentile = (0..9)
                            // percentile iteration at several units-per-tick
                            .map { 1.shl(it) }
                            .associateBy({ "$it" },
                                    { transform(h.percentiles(it)) { IteratorValue(it!!) } }),
                    logarithmic = listOf(1L, 100, 1000)
                            // There are two ways to vary a logarithmic iteration: the first bucket size, and the log
                            // base. So, we map pairs of first bucket and log bases into "firstbucket-logbase" names to
                            // use in the JSON.
                            .flatMap { firstBucketSize ->
                                listOf(1.1, 2.0, 10.0).map { logBase ->
                                    Pair(firstBucketSize, logBase)
                                }
                            }
                            .associateBy({ "${it.first}-${it.second}" }, {
                                transform(h.logarithmicBucketValues(it.first, it.second)) { IteratorValue(it!!) }
                            }),
                    recorded = transform(h.recordedValues()) { IteratorValue(it!!) },
                    all = transform(h.allValues()) { IteratorValue(it!!) }
            ))
}

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

class Iterators(@JsonProperty("linear") val linear: Map<String, Iterable<IteratorValue>>,
                @JsonProperty("percentile") val percentile: Map<String, Iterable<IteratorValue>>,
                @JsonProperty("logarithmic") val logarithmic: Map<String, Iterable<IteratorValue>>,
                @JsonProperty("recorded") val recorded: Iterable<IteratorValue>,
                @JsonProperty("all") val all: Iterable<IteratorValue>
)
