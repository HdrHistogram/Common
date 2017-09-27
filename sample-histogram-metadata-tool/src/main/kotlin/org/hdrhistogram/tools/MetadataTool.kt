package org.hdrhistogram.tools

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectWriter
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
import java.lang.Math.log
import java.lang.Math.max
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

        var futuresSubmitted = 0

        Files.newDirectoryStream(dir.toPath(), "*.histo").use { stream ->
            stream.map { path ->
                service.submit({
                    println("Starting $path")
                    val buf = ByteBuffer.wrap(Files.readAllBytes(path))
                    val histo = Histogram.decodeFromCompressedByteBuffer(buf, 1)

                    val testDataPath = replaceSuffix(path, Regex("\\.histo$"), "-test-data.json.xz")
                    writeToPath(testDataPath, jsonWriter, HistogramTestData(histo))

                    testDataPath
                })

                futuresSubmitted += 1
            }
        }

        Files.newDirectoryStream(dir.toPath(), "*.hlog").use { stream ->
            stream.map { path ->
                println("Starting $path")
                val reader = HistogramLogReader(path.toFile())

                val logTestDataDir = replaceSuffix(path, Regex("\\.hlog$"), "-test-data")
                logTestDataDir.toFile().mkdir()

                var index = 0

                while (reader.hasNext()) {
                    val h = reader.nextIntervalHistogram() as AbstractHistogram
                    val testDataPath = logTestDataDir.resolve("$index.json.xz")

                    service.submit {
                        writeToPath(testDataPath, jsonWriter, HistogramLogEntry(h))

                        testDataPath
                    }

                    futuresSubmitted += 1

                    index += 1
                }
            }
        }

        for (i in 0 until futuresSubmitted) {
            val f = service.take()
            println("Finished ${f.get()}")
        }

        pool.shutdown()

        println("Elapsed time: ${Duration.between(start, Instant.now())}")
    }

}

private fun writeToPath(path: Path, jsonWriter: ObjectWriter, obj: Any) {
    Files.newOutputStream(path).use { fos ->
        XZOutputStream(fos, LZMA2Options()).use { xzos ->
            jsonWriter.writeValue(xzos, obj)
        }
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
                    // pick a stride suitable for the size of the histogram so we don't use small strides on mega histos
                    linear = (max(log2(h.maxValue) - 10, 1)..log2(h.maxValue))
                            // linear stride with strides at several powers of 2
                            .map { 1L.shl(it) }
                            .associateBy({ "$it" },
                                    { stride -> transform(h.linearBucketValues(stride)) { IteratorValue(it!!) } }),
                    percentile = (0..9)
                            // percentile iteration at several units-per-tick
                            .map { 1.shl(it) }
                            .associateBy({ "$it" },
                                    { unitsPerTick -> transform(h.percentiles(unitsPerTick)) { IteratorValue(it!!) } }),
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

private fun log2(l: Long) = (log(l.toDouble()) / log(2.0)).toInt()

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
