package fr.insee.vtl.model;

/**
 * @param <C>
 * @param <T>
 * @param <V>
 */
public interface AggregationExpression<C, T, V> extends TypedExpression {

    /**
     * Initialize the aggregation, called once per core by the collector.
     */
    C init();

    /**
     * Accumulate one value into the instance returned by the init() method.
     */
    void accept(C c, T o);

    /**
     * Combine
     *
     * @param c1
     * @param c2
     * @return
     */
    C combine(C c1, C c2);

    Object finish(C c);

    ResolvableExpression getExpression();

}