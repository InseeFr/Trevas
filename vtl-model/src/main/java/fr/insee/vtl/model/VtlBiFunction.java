package fr.insee.vtl.model;

import java.io.Serializable;
import java.util.function.BiFunction;

/** A serializable BiFunction. */
@FunctionalInterface
public interface VtlBiFunction<T, U, R> extends BiFunction<T, U, R>, Serializable {}
