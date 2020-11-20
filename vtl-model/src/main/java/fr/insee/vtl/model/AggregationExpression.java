package fr.insee.vtl.model;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class AggregationExpression implements Collector<Structured.DataPoint, Object, Object>, TypedExpression {

    private final Collector<Structured.DataPoint, ?, ? extends Object> aggregation;
    private final Class<?> type;

    public AggregationExpression(Collector<Structured.DataPoint, ?, ? extends Object> aggregation, Class<?> type) {
        this.aggregation = aggregation;
        this.type = type;
    }

    public static AggregationExpression count() {
        return withType(Collectors.counting(), Long.class);
    }

    public static AggregationExpression avg(ResolvableExpression expression) {
        if (Long.class.equals(expression.getType())) {
            return withExpression(expression, Collectors.averagingLong(value -> (Long) value), Double.class);
        } else if (Double.class.equals(expression.getType())) {
            return withExpression(expression, Collectors.averagingDouble(value -> (Double) value), Double.class);
        } else {
            // TODO
            throw new Error();
        }
    }


    public static AggregationExpression sum(ResolvableExpression expression) {
        if (Long.class.equals(expression.getType())) {
            return withExpression(expression, Collectors.summingLong(value -> (Long) value), Long.class);
        } else if (Double.class.equals(expression.getType())) {
            return withExpression(expression, Collectors.summingDouble(value -> (Double) value), Double.class);
        } else {
            // TODO
            throw new Error();
        }
    }

    public static AggregationExpression withType(Collector<Structured.DataPoint, ?, ?> collector, Class<?> type) {
        return new AggregationExpression(collector, type);
    }

    public static AggregationExpression withExpression(ResolvableExpression expression, Collector<Object, ?, ?> collector, Class<?> type) {
        return new AggregationExpression(Collectors.mapping(new Function<Structured.DataPoint, Object>() {
            @Override
            public Object apply(Structured.DataPoint dataPoint) {
                // TODO: Use datapoint instead.
                return expression.resolve(dataPoint);
            }
        }, collector), type);
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