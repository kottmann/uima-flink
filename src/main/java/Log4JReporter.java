/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Log4JReporter implements MetricReporter, Scheduled {
  private static Logger LOG = LoggerFactory.getLogger(Log4JReporter.class);

  private final Map<Counter, String> counters = new HashMap<>();
  private final Map<Gauge<?>, String> gauges = new HashMap();
  private final Map<Histogram, String> histograms = new HashMap();
  private final Map<Meter, String> meters = new HashMap();

  @Override
  public void open(MetricConfig metricConfig) {
  }

  @Override
  public void close() {
  }

  @Override
  public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
    String name = group.getMetricIdentifier(metricName);
    synchronized (this) {
      if (metric instanceof Counter) {
        counters.put((Counter) metric, name);
      } else if (metric instanceof Gauge<?>) {
        gauges.put((Gauge<?>) metric, name);
      } else if (metric instanceof Meter) {
        meters.put((Meter) metric, name);
      } else if (metric instanceof Histogram) {
        histograms.put((Histogram) metric, name);
      }
    }
  }

  @Override
  public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
    synchronized (this) {
      if (metric instanceof Counter) {
        counters.remove(metric);
      } else if (metric instanceof Gauge<?>) {
        this.gauges.remove(metric);
      } else if (metric instanceof Histogram) {
        this.histograms.remove(metric);
      } else if (metric instanceof Meter) {
        this.meters.remove(metric);
      }
    }
  }

  @Override
  public void report() {
    LOG.info("========= Starting metric report, T={} =========", System.currentTimeMillis());
    for (Map.Entry<Counter, String> metric : counters.entrySet()) {
      LOG.info("{}: {}", metric.getValue(), metric.getKey());
    }
    for (Map.Entry<Gauge<?>, String> metric : gauges.entrySet()) {
      LOG.info("{}: {}", metric.getValue(), metric.getKey().getValue().toString());
    }
    for (Map.Entry<Meter, String> metric : meters.entrySet()) {
      LOG.info("{}: {}", metric.getValue(), metric.getKey().getRate());
    }
    for (Map.Entry<Histogram, String> metric : histograms.entrySet()) {
      HistogramStatistics stats = metric.getKey().getStatistics();
      LOG.info("{}: count:{} min:{} max:{} mean:{} stddev:{} p50:{} p75:{} p95:{} p98:{} p99:{} p999:{}",
          metric.getValue(), stats.size(), stats.getMin(), stats.getMax(), stats.getMean(), stats.getStdDev(),
          stats.getQuantile(0.50), stats.getQuantile(0.75), stats.getQuantile(0.95),
          stats.getQuantile(0.98), stats.getQuantile(0.99), stats.getQuantile(0.999));
    }
    LOG.info("========= Finished metric report =========");
  }
}