package fr.insee.vtl.model;

import java.util.stream.Collector;

/** Online mean / variance accumulator (Welford), mergeable for parallel aggregation. */
final class WelfordAccumulator {

  private long count;
  private double mean;
  private double m2;
  private boolean hasNull;

  static Collector<Long, WelfordAccumulator, Double> longVarianceCollector(boolean population) {
    return Collector.of(
        WelfordAccumulator::new,
        WelfordAccumulator::add,
        WelfordAccumulator::merge,
        state -> state.finishVariance(population));
  }

  static Collector<Double, WelfordAccumulator, Double> doubleVarianceCollector(boolean population) {
    return Collector.of(
        WelfordAccumulator::new,
        WelfordAccumulator::add,
        WelfordAccumulator::merge,
        state -> state.finishVariance(population));
  }

  static Collector<Long, WelfordAccumulator, Double> longStdDevCollector(boolean population) {
    return Collector.of(
        WelfordAccumulator::new,
        WelfordAccumulator::add,
        WelfordAccumulator::merge,
        state -> state.finishStdDev(population));
  }

  static Collector<Double, WelfordAccumulator, Double> doubleStdDevCollector(boolean population) {
    return Collector.of(
        WelfordAccumulator::new,
        WelfordAccumulator::add,
        WelfordAccumulator::merge,
        state -> state.finishStdDev(population));
  }

  void add(Long value) {
    if (value == null) {
      hasNull = true;
      return;
    }
    add(value.doubleValue());
  }

  void add(Double value) {
    if (value == null) {
      hasNull = true;
      return;
    }
    add(value.doubleValue());
  }

  private void add(double value) {
    count++;
    double delta = value - mean;
    mean += delta / count;
    double delta2 = value - mean;
    m2 += delta * delta2;
  }

  WelfordAccumulator merge(WelfordAccumulator other) {
    if (other.hasNull) {
      hasNull = true;
    }
    if (other.count == 0) {
      return this;
    }
    if (count == 0) {
      count = other.count;
      mean = other.mean;
      m2 = other.m2;
      return this;
    }
    long combined = count + other.count;
    double delta = other.mean - mean;
    mean = mean + delta * other.count / combined;
    m2 = m2 + other.m2 + delta * delta * count * other.count / combined;
    count = combined;
    return this;
  }

  Double finishVariance(boolean population) {
    if (hasNull) {
      return null;
    }
    if (count <= 1) {
      return 0D;
    }
    double divisor = population ? count : (count - 1);
    return m2 / divisor;
  }

  Double finishStdDev(boolean population) {
    Double variance = finishVariance(population);
    if (variance == null) {
      return null;
    }
    return Math.sqrt(variance);
  }
}
