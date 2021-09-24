package fr.insee.vtl.engine.utils;

import fr.insee.vtl.model.Structured;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * Collector that uses a map of collectors.
 */
public class MapCollector implements Collector<Structured.DataPoint, Structured.DataPoint, Structured.DataPoint> {

    private final Structured.DataStructure structure;
    private final Map<String, Supplier<Object>> supplierMap = new HashMap<>();
    private final Map<String, BiConsumer<Object, Structured.DataPoint>> accumulatorMap = new HashMap<>();
    private final Map<String, BinaryOperator<Object>> combinerMap = new HashMap<>();
    private final Map<String, Function<Object, Object>> finisherMap = new HashMap<>();

    public MapCollector(Structured.DataStructure structure, Map<String, ? extends Collector<Structured.DataPoint, Object, Object>> collectorMap) {
        this.structure = Objects.requireNonNull(structure);
        if (!structure.keySet().containsAll(collectorMap.keySet())) {
            throw new IllegalArgumentException("inconsistent collector map");
        }
        for (Map.Entry<String, ? extends Collector<Structured.DataPoint, Object, Object>> entry : collectorMap.entrySet()) {
            supplierMap.put(entry.getKey(), entry.getValue().supplier());
            accumulatorMap.put(entry.getKey(), entry.getValue().accumulator());
            combinerMap.put(entry.getKey(), entry.getValue().combiner());
            finisherMap.put(entry.getKey(), entry.getValue().finisher());
        }
    }

    @Override
    public Supplier<Structured.DataPoint> supplier() {
        return () -> {
            Structured.DataPoint dataPoint = new Structured.DataPoint(structure);
            for (Map.Entry<String, Supplier<Object>> entry : supplierMap.entrySet()) {
                String column = entry.getKey();
                dataPoint.set(column, entry.getValue().get());
            }
            return dataPoint;
        };
    }

    @Override
    public BiConsumer<Structured.DataPoint, Structured.DataPoint> accumulator() {
        return (map, context) -> {
            for (Map.Entry<String, BiConsumer<Object, Structured.DataPoint>> entry : accumulatorMap.entrySet()) {
                String column = entry.getKey();
                Object accumulatorValue = map.get(column);
                entry.getValue().accept(accumulatorValue, context);
            }
        };
    }

    @Override
    public BinaryOperator<Structured.DataPoint> combiner() {
        return (map, map2) -> {
            for (Map.Entry<String, BinaryOperator<Object>> entry : combinerMap.entrySet()) {
                String column = entry.getKey();
                Object newValue = entry.getValue().apply(map.get(column), map2.get(column));
                map.set(column, newValue);
            }
            return map;
        };
    }

    @Override
    public Function<Structured.DataPoint, Structured.DataPoint> finisher() {
        return map -> {
            for (Map.Entry<String, Function<Object, Object>> entry : finisherMap.entrySet()) {
                String column = entry.getKey();
                map.set(column, entry.getValue().apply(map.get(column)));
            }
            return map;
        };
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of(Characteristics.UNORDERED);
    }
}
