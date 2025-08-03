package fr.insee.vtl.model;

import java.time.Instant;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * The <code>AggregationExpression</code> class is an abstract representation of an aggregation
 * expression.
 */
public class AggregationExpression
    implements Collector<Structured.DataPoint, Object, Object>, TypedExpression {

  private final Collector<Structured.DataPoint, ?, ?> aggregation;
  private final Class<?> type;

  /**
   * Constructor taking a collector of data points and an intended type for the aggregation results.
   *
   * @param aggregation Collector of data points.
   * @param type Expected type for aggregation results.
   */
  public <T> AggregationExpression(
      Collector<Structured.DataPoint, ?, T> aggregation, Class<T> type) {
    this.aggregation = aggregation;
    this.type = type;
  }

  /**
   * Constructor based on an input expression, a data point collector and an expected type. The
   * input expression is applied to each data point before it is accepted by the data point
   * collector.
   *
   * @param expression The input resolvable expression.
   * @param collector The data point collector.
   * @param type The expected type of the aggregation expression results.
   */
  public <T> AggregationExpression(
      ResolvableExpression expression, Collector<Object, ?, T> collector, Class<T> type) {
    this(Collectors.mapping(expression::resolve, collector), type);
  }

  /**
   * Returns an aggregation expression that counts data points and returns a long integer.
   *
   * @return The counting expression.
   */
  public static AggregationExpression count() {
    return new CountAggregationExpression(Collectors.counting(), Long.class);
  }

  public static class CountAggregationExpression extends AggregationExpression {
    private <T> CountAggregationExpression(
        Collector<Structured.DataPoint, ?, T> aggregation, Class<T> type) {
      super(aggregation, type);
    }
  }

  /**
   * Returns an aggregation expression that averages an expression on data points and returns a
   * double number.
   *
   * @param expression The expression on data points.
   * @return The averaging expression.
   */
  public static AggregationExpression avg(ResolvableExpression expression) {
    if (Long.class.equals(expression.getType())) {
      return new AverageAggregationExpression(
          expression, Collectors.averagingLong(value -> (Long) value), Double.class);
    } else if (Double.class.equals(expression.getType())) {
      return new AverageAggregationExpression(
          expression, Collectors.averagingDouble(value -> (Double) value), Double.class);
    } else {
      // Type asserted in visitor.
      throw new Error("unexpected type");
    }
  }

  public static class AverageAggregationExpression extends AggregationExpression {
    public <T> AverageAggregationExpression(
        ResolvableExpression expression, Collector<Object, ?, T> collector, Class<T> type) {
      super(expression, collector, type);
    }
  }

  /**
   * Returns an aggregation expression that sums an expression on data points and returns a long
   * integer or double number.
   *
   * @param expression The expression on data points.
   * @return The summing expression.
   */
  public static AggregationExpression sum(ResolvableExpression expression) {
    if (Long.class.equals(expression.getType())) {
      return new SumAggregationExpression(
          expression, Collectors.summingLong(value -> (Long) value), Long.class);
    } else if (Double.class.equals(expression.getType())) {
      return new SumAggregationExpression(
          expression, Collectors.summingDouble(value -> (Double) value), Double.class);
    } else {
      // Type asserted in visitor.
      throw new Error("unexpected type");
    }
  }

  public static class SumAggregationExpression extends AggregationExpression {
    public <T> SumAggregationExpression(
        ResolvableExpression expression, Collector<Object, ?, T> collector, Class<T> type) {
      super(expression, collector, type);
    }
  }

  /**
   * Returns an aggregation expression that give median of an expression on data points and returns
   * a double number.
   *
   * @param expression The expression on data points.
   * @return The median expression.
   */
  public static AggregationExpression median(ResolvableExpression expression) {
    if (Long.class.equals(expression.getType())) {
      return new MedianAggregationExpression(
          expression, Collectors.mapping(v -> (Long) v, medianCollectorLong()), Double.class);
    } else if (Double.class.equals(expression.getType())) {
      return new MedianAggregationExpression(
          expression, Collectors.mapping(v -> (Double) v, medianCollectorDouble()), Double.class);
    } else {
      // Type asserted in visitor.
      throw new Error("unexpected type");
    }
  }

  public static class MedianAggregationExpression extends AggregationExpression {
    public <T> MedianAggregationExpression(
        ResolvableExpression expression, Collector<Object, ?, T> collector, Class<T> type) {
      super(expression, collector, type);
    }
  }

  /**
   * Returns an aggregation expression that give max of an expression on data points and returns a
   * long integer or double number.
   *
   * @param expression The expression on data points.
   * @return The max expression.
   */
  public static AggregationExpression max(ResolvableExpression expression) {
    Class<?> type = expression.getType();

    if (Long.class.equals(type)) {
      return maxAggregation(expression, Long.class);
    } else if (Double.class.equals(type)) {
      return maxAggregation(expression, Double.class);
    } else if (String.class.equals(type)) {
      return maxAggregation(expression, String.class);
    } else if (Boolean.class.equals(type)) {
      return maxAggregation(expression, Boolean.class);
    } else if (Instant.class.equals(type)) {
      return maxAggregation(expression, Instant.class);
    } else {
      throw new Error("unexpected type");
    }
  }

  private static <T extends Comparable<? super T>> AggregationExpression maxAggregation(
      ResolvableExpression expression, Class<T> type) {
    Comparator<T> comparator = Comparator.nullsFirst(Comparator.naturalOrder());
    Collector<Object, ?, T> collector =
        Collectors.collectingAndThen(
            Collectors.mapping(type::cast, Collectors.maxBy(comparator)), opt -> opt.orElse(null));
    return new MaxAggregationExpression(expression, collector, type);
  }

  public static class MaxAggregationExpression extends AggregationExpression {
    public <T> MaxAggregationExpression(
        ResolvableExpression expression, Collector<Object, ?, T> collector, Class<T> type) {
      super(expression, collector, type);
    }
  }

  /**
   * Returns an aggregation expression that give min of an expression on data points and returns a
   * long integer or double number.
   *
   * @param expression The expression on data points.
   * @return The min expression.
   */
  public static AggregationExpression min(ResolvableExpression expression) {
    Class<?> type = expression.getType();

    if (Long.class.equals(type)) {
      return minAggregation(expression, Long.class);
    } else if (Double.class.equals(type)) {
      return minAggregation(expression, Double.class);
    } else if (String.class.equals(type)) {
      return minAggregation(expression, String.class);
    } else if (Boolean.class.equals(type)) {
      return minAggregation(expression, Boolean.class);
    } else if (Instant.class.equals(type)) {
      return minAggregation(expression, Instant.class);
    } else {
      throw new Error("unexpected type");
    }
  }

  private static <T extends Comparable<? super T>> AggregationExpression minAggregation(
      ResolvableExpression expression, Class<T> type) {
    Comparator<T> comparator = Comparator.nullsFirst(Comparator.naturalOrder());
    Collector<Object, ?, T> collector =
        Collectors.collectingAndThen(
            Collectors.mapping(type::cast, Collectors.minBy(comparator)), opt -> opt.orElse(null));
    return new MinAggregationExpression(expression, collector, type);
  }

  public static class MinAggregationExpression extends AggregationExpression {
    public <T> MinAggregationExpression(
        ResolvableExpression expression, Collector<Object, ?, T> collector, Class<T> type) {
      super(expression, collector, type);
    }
  }

  /**
   * Returns an aggregation expression that give population standard deviation of an expression on
   * data points and returns a double number.
   *
   * @param expression The expression on data points.
   * @return The population standard deviation expression.
   */
  public static AggregationExpression stdDevPop(ResolvableExpression expression) {
    if (Long.class.equals(expression.getType())) {
      return new StdDevPopAggregationExpression(
          expression, Collectors.mapping(v -> (Long) v, stdDevPopCollectorLong()), Double.class);
    } else if (Double.class.equals(expression.getType())) {
      return new StdDevPopAggregationExpression(
          expression,
          Collectors.mapping(v -> (Double) v, stdDevPopCollectorDouble()),
          Double.class);
    } else {
      // Type asserted in visitor.
      throw new Error("unexpected type");
    }
  }

  public static class StdDevPopAggregationExpression extends AggregationExpression {
    public <T> StdDevPopAggregationExpression(
        ResolvableExpression expression, Collector<Object, ?, T> collector, Class<T> type) {
      super(expression, collector, type);
    }
  }

  /**
   * Returns an aggregation expression that give sample standard deviation of an expression on data
   * points and returns a double number.
   *
   * @param expression The expression on data points.
   * @return The sample standard deviation expression.
   */
  public static AggregationExpression stdDevSamp(ResolvableExpression expression) {
    if (Long.class.equals(expression.getType())) {
      return new StdDevSampAggregationExpression(
          expression, Collectors.mapping(v -> (Long) v, stdDevSampCollectorLong()), Double.class);
    } else if (Double.class.equals(expression.getType())) {
      return new StdDevSampAggregationExpression(
          expression,
          Collectors.mapping(v -> (Double) v, stdDevSampCollectorDouble()),
          Double.class);
    } else {
      // Type asserted in visitor.
      throw new Error("unexpected type");
    }
  }

  public static class StdDevSampAggregationExpression extends AggregationExpression {
    public <T> StdDevSampAggregationExpression(
        ResolvableExpression expression, Collector<Object, ?, T> collector, Class<T> type) {
      super(expression, collector, type);
    }
  }

  /**
   * Returns an aggregation expression that give population variance of an expression on data points
   * and returns a double number.
   *
   * @param expression The expression on data points.
   * @return The population variance expression.
   */
  public static AggregationExpression varPop(ResolvableExpression expression) {
    if (Long.class.equals(expression.getType())) {
      return new VarPopAggregationExpression(
          expression, Collectors.mapping(v -> (Long) v, varPopCollectorLong()), Double.class);
    } else if (Double.class.equals(expression.getType())) {
      return new VarPopAggregationExpression(
          expression, Collectors.mapping(v -> (Double) v, varPopCollectorDouble()), Double.class);
    } else {
      // Type asserted in visitor.
      throw new Error("unexpected type");
    }
  }

  public static class VarPopAggregationExpression extends AggregationExpression {
    public <T> VarPopAggregationExpression(
        ResolvableExpression expression, Collector<Object, ?, T> collector, Class<T> type) {
      super(expression, collector, type);
    }
  }

  /**
   * Returns an aggregation expression that give sample variance of an expression on data points and
   * returns a double number.
   *
   * @param expression The expression on data points.
   * @return The sample variance expression.
   */
  public static AggregationExpression varSamp(ResolvableExpression expression) {
    if (Long.class.equals(expression.getType())) {
      return new VarSampAggregationExpression(
          expression, Collectors.mapping(v -> (Long) v, varSampCollectorLong()), Double.class);
    } else if (Double.class.equals(expression.getType())) {
      return new VarSampAggregationExpression(
          expression, Collectors.mapping(v -> (Double) v, varSampCollectorDouble()), Double.class);
    } else {
      // Type asserted in visitor.
      throw new Error("unexpected type");
    }
  }

  public static class VarSampAggregationExpression extends AggregationExpression {
    public <T> VarSampAggregationExpression(
        ResolvableExpression expression, Collector<Object, ?, T> collector, Class<T> type) {
      super(expression, collector, type);
    }
  }

  private static Collector<Long, List<Long>, Double> medianCollectorLong() {
    return Collector.of(
        ArrayList::new,
        List::add,
        (longs, longs2) -> {
          longs.addAll(longs2);
          return longs;
        },
        longs -> {
          if (longs.contains(null)) return null;
          Collections.sort(longs);
          if (longs.size() % 2 == 0) {
            return (double) (longs.get(longs.size() / 2 - 1) + longs.get(longs.size() / 2)) / 2;
          } else {
            return (double) longs.get(longs.size() / 2);
          }
        });
  }

  private static Collector<Double, List<Double>, Double> medianCollectorDouble() {
    return Collector.of(
        ArrayList::new,
        List::add,
        (longs, longs2) -> {
          longs.addAll(longs2);
          return longs;
        },
        longs -> {
          if (longs.contains(null)) return null;
          Collections.sort(longs);
          if (longs.size() % 2 == 0) {
            return (longs.get(longs.size() / 2 - 1) + longs.get(longs.size() / 2)) / 2;
          } else {
            return longs.get(longs.size() / 2);
          }
        });
  }

  private static Collector<Long, List<Long>, Double> stdDevPopCollectorLong() {
    return Collector.of(
        ArrayList::new,
        List::add,
        (longs, longs2) -> {
          longs.addAll(longs2);
          return longs;
        },
        getDeviationLongFn(true));
  }

  private static Collector<Double, List<Double>, Double> stdDevPopCollectorDouble() {
    return Collector.of(
        ArrayList::new,
        List::add,
        (longs, longs2) -> {
          longs.addAll(longs2);
          return longs;
        },
        getDeviationDoubleFn(true));
  }

  private static Collector<Long, List<Long>, Double> stdDevSampCollectorLong() {
    return Collector.of(
        ArrayList::new,
        List::add,
        (longs, longs2) -> {
          longs.addAll(longs2);
          return longs;
        },
        getDeviationLongFn(false));
  }

  private static Collector<Double, List<Double>, Double> stdDevSampCollectorDouble() {
    return Collector.of(
        ArrayList::new,
        List::add,
        (longs, longs2) -> {
          longs.addAll(longs2);
          return longs;
        },
        getDeviationDoubleFn(false));
  }

  private static Collector<Long, List<Long>, Double> varPopCollectorLong() {
    return Collector.of(
        ArrayList::new,
        List::add,
        (longs, longs2) -> {
          longs.addAll(longs2);
          return longs;
        },
        getVarLongFn(true));
  }

  private static Collector<Double, List<Double>, Double> varPopCollectorDouble() {
    return Collector.of(
        ArrayList::new,
        List::add,
        (longs, longs2) -> {
          longs.addAll(longs2);
          return longs;
        },
        getVarDoubleFn(true));
  }

  private static Collector<Long, List<Long>, Double> varSampCollectorLong() {
    return Collector.of(
        ArrayList::new,
        List::add,
        (longs, longs2) -> {
          longs.addAll(longs2);
          return longs;
        },
        getVarLongFn(false));
  }

  private static Collector<Double, List<Double>, Double> varSampCollectorDouble() {
    return Collector.of(
        ArrayList::new,
        List::add,
        (longs, longs2) -> {
          longs.addAll(longs2);
          return longs;
        },
        getVarDoubleFn(false));
  }

  private static Function<List<Long>, Double> getDeviationLongFn(Boolean usePopulation) {
    return longs -> {
      if (longs.contains(null)) return null;
      if (longs.size() <= 1) return 0D;
      Double avg = longs.stream().collect(Collectors.averagingLong(v -> v));
      return Math.sqrt(
          longs.stream().map(v -> Math.pow(((double) v) - avg, 2)).mapToDouble(v -> v).sum()
              / (longs.size() - (usePopulation ? 0D : 1D)));
    };
  }

  private static Function<List<Double>, Double> getDeviationDoubleFn(Boolean usePopulation) {
    return doubles -> {
      if (doubles.contains(null)) return null;
      if (doubles.size() <= 1) return 0D;
      Double avg = doubles.stream().collect(Collectors.averagingDouble(v -> v));
      return Math.sqrt(
          doubles.stream().map(v -> Math.pow(v - avg, 2)).mapToDouble(v -> v).sum()
              / (doubles.size() - (usePopulation ? 0D : 1D)));
    };
  }

  private static Function<List<Long>, Double> getVarLongFn(Boolean usePopulation) {
    return longs -> {
      if (longs.contains(null)) return null;
      if (longs.size() <= 1) return 0D;
      Double avg = longs.stream().collect(Collectors.averagingLong(v -> v));
      return longs.stream().map(v -> Math.pow(((double) v) - avg, 2)).mapToDouble(v -> v).sum()
          / (longs.size() - (usePopulation ? 0D : 1D));
    };
  }

  private static Function<List<Double>, Double> getVarDoubleFn(Boolean usePopulation) {
    return doubles -> {
      if (doubles.contains(null)) return null;
      if (doubles.size() <= 1) return 0D;
      Double avg = doubles.stream().collect(Collectors.averagingDouble(v -> v));
      return doubles.stream().map(v -> Math.pow(v - avg, 2)).mapToDouble(v -> v).sum()
          / (doubles.size() - (usePopulation ? 0D : 1D));
    };
  }

  @Override
  public Class<?> getType() {
    return type;
  }

  @Override
  public Supplier<Object> supplier() {
    return (Supplier<Object>) aggregation.supplier();
  }

  @Override
  public BiConsumer<Object, Structured.DataPoint> accumulator() {
    return (BiConsumer<Object, Structured.DataPoint>) aggregation.accumulator();
  }

  @Override
  public BinaryOperator<Object> combiner() {
    return (BinaryOperator<Object>) aggregation.combiner();
  }

  @Override
  public Function<Object, Object> finisher() {
    return (Function<Object, Object>) aggregation.finisher();
  }

  @Override
  public Set<Characteristics> characteristics() {
    return aggregation.characteristics();
  }
}
