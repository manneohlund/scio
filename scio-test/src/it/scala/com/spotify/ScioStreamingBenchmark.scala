/*
 * Copyright 2018 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify

import java.lang.management.ManagementFactory

import com.google.api.services.dataflow.model.Job
import com.google.cloud.monitoring.v3.MetricServiceClient
import com.google.common.reflect.ClassPath
import com.google.monitoring.v3.ListTimeSeriesRequest.TimeSeriesView
import com.google.monitoring.v3.{ProjectName, TimeInterval}
import com.google.protobuf.Timestamp
import com.spotify.ScioBenchmarkSettings.circleCIEnv
import com.spotify.scio._
import com.sun.management.OperatingSystemMXBean
import org.apache.beam.sdk.io.GenerateSequence
import org.apache.beam.sdk.options.StreamingOptions
import org.joda.time._
import org.joda.time.format.DateTimeFormat
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
 * Streaming benchmark jobs, restarted daily and polled for metrics every hour.
 *
 * This file is symlinked to scio-bench/src/main/scala/com/spotify/ScioStreamingBenchmark.scala so
 * that it can run with past Scio releases.
 */
object ScioStreamingBenchmark {

  import DataflowProvider._
  import ScioBenchmarkSettings._

  // Launch the streaming benchmark jobs and cancel old jobs
  def main(args: Array[String]): Unit = {
    val argz = Args(args)
    val name = argz("name")
    val regex = argz.getOrElse("regex", ".*")
    val projectId = argz.getOrElse("project", defaultProjectId)
    val timestamp = DateTimeFormat.forPattern("yyyyMMddHHmmss")
      .withZone(DateTimeZone.UTC)
      .print(System.currentTimeMillis())
    val prefix = s"ScioStreamingBenchmark-$name-$timestamp"

    cancelCurrentJobs(projectId)

    benchmarks
      .filter(_.name.matches(regex))
      .map(_.run(projectId, prefix, commonArgs("n1-highmem-8")))
  }

  private val benchmarks = ClassPath.from(Thread.currentThread().getContextClassLoader)
    .getAllClasses
    .asScala
    .filter(_.getName.matches("com\\.spotify\\.ScioStreamingBenchmark\\$[\\w]+\\$"))
    .flatMap { ci =>
      val cls = ci.load()
      if (classOf[StreamingBenchmark] isAssignableFrom cls) {
        Some(cls.newInstance().asInstanceOf[StreamingBenchmark])
      } else {
        None
      }
    }

  private val cancelledState = "JOB_STATE_CANCELLED"

  /** Cancel any currently running streaming benchmarks before spawning new ones */
  private def cancelCurrentJobs(projectId: String): Unit = {
    val jobs = dataflow.projects().jobs()

    Option(jobs.list(projectId).setFilter("ACTIVE").execute().getJobs).foreach { activeJobs =>
      activeJobs.asScala.foreach { job =>
        if (job.getName.toLowerCase.startsWith("sciostreamingbenchmark")) {
          PrettyPrint.print("CancelCurrentJobs", s"Stopping job.... ${job.getName}")

          jobs.update(
            projectId,
            job.getId,
            new Job().setProjectId(projectId).setId(job.getId).setRequestedState(cancelledState)
          ).execute()
        }
      }
    }
  }

  // =======================================================================
  // Benchmarks
  // =======================================================================

  // @Todo write actual benchmark job
  object StreamingBenchmarkExample extends StreamingBenchmark {
    override def run(sc: ScioContext): Unit = {
      sc.optionsAs[StreamingOptions].setStreaming(true)
      sc
        .customInput(
          "testJob",
          GenerateSequence
            .from(0)
            .withRate(1, org.joda.time.Duration.standardSeconds(10))
        )
        .withFixedWindows(
          duration = org.joda.time.Duration.standardSeconds(10),
          offset = org.joda.time.Duration.ZERO)
        .map { x =>
          (1 to 1000).sorted
          logMemoryMetrics()
        }
    }
  }

}

object ScioStreamingBenchmarkMetrics {

  import DataflowProvider._
  import ScioBenchmarkSettings._

  // Triggered once an hour to poll job metrics from currently running streaming benchmarks
  def main(args: Array[String]): Unit = {
    val argz = Args(args)
    val name = argz("name")
    val projectId = argz.getOrElse("project", defaultProjectId)
    val jobNamePattern = s"sciostreamingbenchmark-$name-\\d+-([a-zA-Z0-9]+)-(\\d+)-\\w+".r

    val jobs = dataflow.projects().jobs().list(projectId)

    printTimeSeries(projectId)

    val hourlyMetrics =
      for (
        activeJobs <- Option(jobs.setFilter("ACTIVE").execute().getJobs);
        job <- activeJobs.asScala;
        benchmarkNameAndBuildNum <- jobNamePattern.findFirstMatchIn(job.getName)
      ) yield {
        BenchmarkResult.streaming(
          benchmarkNameAndBuildNum.group(1),
          benchmarkNameAndBuildNum.group(2).toLong,
          job.getCreateTime,
          job.getId,
          dataflow.projects().jobs().getMetrics(projectId, job.getId).execute()
        )
      }

    new DatastoreStreamingLogger().log(hourlyMetrics)
  }

  private def timeFormat(seconds: Long): String = {
    new LocalDateTime(seconds * 1000).toString
  }

  // @Todo update with real finalized metric names...
  private def printTimeSeries(projectId: String): Unit = {
    val startTime = new LocalDateTime().minusHours(1).toDate.toInstant.toEpochMilli

    MetricServiceClient.create()
      .listTimeSeries(
        ProjectName.newBuilder().setProject(projectId).build(),
        "metric.type=\"logging.googleapis.com/user/heap_usage\"",
        TimeInterval.newBuilder()
          .setStartTime(Timestamp.newBuilder().setSeconds(startTime / 1000).build())
          .setEndTime(Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000).build())
          .build(),
        TimeSeriesView.FULL
      ).iterateAll().asScala.foreach { x =>
      print(s"\nMetric: ${x.getMetric.getLabelsMap.get("benchmark_name")} / ${x.getMetric.getType}")
      x.getPointsList.asScala.foreach { point =>
        print(s"\n[${timeFormat(point.getInterval.getStartTime.getSeconds)} - " +
          s"${timeFormat(point.getInterval.getEndTime.getSeconds)}] : " +
          s"${point.getValue.getDistributionValue.getMean}")
      }
    }
  }
}

abstract class StreamingBenchmark {
  import ScioBenchmarkSettings.circleCIEnv

  val name: String = this.getClass.getSimpleName.replaceAll("\\$$", "")
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def run(projectId: String,
          prefix: String,
          args: Array[String]): (String, ScioResult) = {
    val username = sys.props("user.name")
    val buildNum = circleCIEnv.map(_.buildNum).getOrElse(-1)

    val (sc, _) = ContextAndArgs(Array(s"--project=$projectId") ++ args)
    sc.setAppName(name)
    sc.setJobName(s"$prefix-$name-$buildNum-$username".toLowerCase())

    run(sc)

    (name, sc.close())
  }

  def run(sc: ScioContext): Unit

  // These logs get converted to labeled Stackdriver metrics via regex matching
  // setup in Stackdriver UI
  def logMemoryMetrics(): Unit = {
    val osBean = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
    val memoryBean = ManagementFactory.getMemoryMXBean
    val label = s"(bench:$name)"

    logger.info(s"$label CPU Usage: ${osBean.getSystemCpuLoad * 100.0}")

    val pMemUsage = (osBean.getTotalPhysicalMemorySize -
      osBean.getFreePhysicalMemorySize).toDouble / osBean.getTotalPhysicalMemorySize
    logger.info(s"$label PMem Usage: ${pMemUsage * 100.0}")

    val heapMemory = memoryBean.getHeapMemoryUsage
    logger.info(s"$label Heap Usage: ${heapMemory.getUsed * 100.0 / heapMemory.getCommitted}")
  }
}

class DatastoreStreamingLogger extends DatastoreLogger(ScioBenchmarkSettings.StreamingMetrics) {
  override def dsKeyId(benchmark: BenchmarkResult): String = {
    val hoursSinceJobLaunch = hourOffset(benchmark.startTime)

    s"${benchmark.buildNum}[$hoursSinceJobLaunch]"
  }

  private def hourOffset(startTime: LocalDateTime): String = {
    val hourOffset = Hours.hoursBetween(startTime, new LocalDateTime(DateTimeZone.UTC)).getHours
    s"+${"%02d".format(hourOffset)}h"
  }
}
