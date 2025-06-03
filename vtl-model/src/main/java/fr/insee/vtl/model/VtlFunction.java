package fr.insee.vtl.model;

import java.io.Serializable;
import java.util.function.Function;

/** A serializable function. */
@FunctionalInterface
public interface VtlFunction<T, R> extends Function<T, R>, Serializable {}
