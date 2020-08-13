package fr.insee.vtl.model;

import java.util.Map;
import java.util.function.Function;

/**
 * The <code>LongExpression</code> class is an abstract representation of an expression of type <code>Long</code>.
 */
public abstract class LongExpression extends NumberExpression {

    /**
     * Returns the result of applying a function of type <code>Long</code> to a given dataset context.
     *
     * @param func A function applicable to a dataset context and yielding a <code>Long</code> result.
     * @return The result of applying the given function to the dataset context.
     */
    @Deprecated
    public static LongExpression withFunction(Function<Map<String, Object>, Long> func) {
        return of(func);
    }

    public static LongExpression of(Long value) {
        return new LongExpression() {
            @Override
            public Long resolve(Map<String, Object> context) {
                return value;
            }
        };
    }

    @Override
    public abstract Long resolve(Map<String, Object> context);

    @Override
    public Class<Long> getType() {
        return Long.class;
    }

    /**
     * Returns the result of applying a function of type <code>Long</code> to a given dataset context.
     *
     * @param func A function applicable to a dataset context and yielding a <code>Long</code> result.
     * @return The result of applying the given function to the dataset context.
     */
    public static LongExpression of(Function<Map<String, Object>, Long> func) {
        return new LongExpression() {
            @Override
            public Long resolve(Map<String, Object> context) {
                return func.apply(context);
            }
        };
    }
}