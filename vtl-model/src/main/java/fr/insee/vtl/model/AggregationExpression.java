package fr.insee.vtl.model;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class AggregationExpression implements Collector<Map<String, Object>, Object, Object>, TypedExpression {

    private final Collector<Map<String, Object>, Object, Object> aggregation;
    private final Class type;

    public AggregationExpression(Collector<Map<String, Object>, Object, Object> aggregation, Class type) {
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

    public static AggregationExpression withType(Collector collector, Class type) {
        return new AggregationExpression(collector, type);
    }

    public static AggregationExpression withExpression(ResolvableExpression expression, Collector collector, Class type) {
        return new AggregationExpression(Collectors.mapping(new Function<Map<String, Object>, Object>() {
            @Override
            public Object apply(Map<String, Object> map) {
                // TODO: Use datapoint instead.
                return expression.resolve(map);
            }
        }, collector), type);
    }

    @Override
    public Class<?> getType() {
        return type;
    }

    @Override
    public Supplier<Object> supplier() {
        return aggregation.supplier();
    }

    @Override
    public BiConsumer<Object, Map<String, Object>> accumulator() {
        return aggregation.accumulator();
    }

    @Override
    public BinaryOperator<Object> combiner() {
        return aggregation.combiner();
    }

    @Override
    public Function<Object, Object> finisher() {
        return aggregation.finisher();
    }

    @Override
    public Set<Characteristics> characteristics() {
        return aggregation.characteristics();
    }
}