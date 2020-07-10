package fr.insee.vtl.model;

import java.util.Map;
import java.util.function.Function;

public abstract class DoubleExpression extends NumberExpression {

    private DoubleExpression() {
    }

    public static DoubleExpression withFunction(Function<Map<String, Object>, Double> func) {
        return new DoubleExpression() {

            @Override
            public Double resolve(Map<String, Object> context) {
                return func.apply(context);
            }
        };
    }

    @Override
    public Class<Double> getType() {
        return Double.class;
    }
}
