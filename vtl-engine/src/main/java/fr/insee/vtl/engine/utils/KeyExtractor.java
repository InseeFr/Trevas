package fr.insee.vtl.engine.utils;

import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.utils.Java8Helpers;

import java.util.*;
import java.util.function.Function;

/**
 * KeyExtractor transform DataPoints to Map.
 * <p>
 * It is useful to compare datapoint based on a set of columns.
 */
public class KeyExtractor implements Function<Structured.DataPoint, Map<String, Object>> {

    private final Set<String> columns;

    public KeyExtractor(Collection<String> columns) {
        this.columns = new HashSet<>(columns);
    }

    @Override
    public Map<String, Object> apply(Structured.DataPoint objects) {
        List<Java8Helpers.MapEntry<String, Object>> entries = new ArrayList<>(objects.size());
        for (String column : columns) {
            entries.add(Java8Helpers.MapEntry.of(column, objects.get(column)));
        }
        return Java8Helpers.mapOfEntries(entries.toArray(new Java8Helpers.MapEntry[0]));
    }
}
