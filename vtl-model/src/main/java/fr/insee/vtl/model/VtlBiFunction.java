package fr.insee.vtl.model;

import java.io.Serializable;
import java.util.function.BiFunction;

@FunctionalInterface
public interface VtlBiFunction<T, U, R> extends BiFunction<T, U, R>, Serializable {
}
