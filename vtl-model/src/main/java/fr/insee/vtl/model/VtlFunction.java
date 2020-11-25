package fr.insee.vtl.model;

import java.io.Serializable;
import java.util.function.Function;

@FunctionalInterface
public interface VtlFunction<T, R> extends Function<T, R>, Serializable {
}
