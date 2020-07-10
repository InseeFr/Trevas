package fr.insee.vtl.model;

import java.util.Map;
import java.util.function.Function;

public abstract class LongExpression extends NumberExpression {

    public LongExpression() {
    }

    public static LongExpression withFunction(Function<Map<String, Object>, Long> func) {
        return new LongExpression() {
            @Override
            public Long resolve(Map<String, Object> context) {
                return func.apply(context);
            }
        };
    }

    @Override
    public abstract Long resolve(Map<String, Object> context);

    @Override
    public Class<Long> getType() {
        return Long.class;
    }
}