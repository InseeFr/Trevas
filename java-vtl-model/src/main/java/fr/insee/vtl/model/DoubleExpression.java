package fr.insee.vtl.model;

import javax.script.ScriptContext;
import java.util.function.Function;

public abstract class DoubleExpression extends NumberExpression {

    private DoubleExpression() {
    }

    public static DoubleExpression withFunction(Function<ScriptContext, Double> func) {
        return new DoubleExpression() {
            @Override
            public Double resolve(ScriptContext context) {
                return func.apply(context);
            }
        };
    }

    @Override
    public abstract Double resolve(ScriptContext context);

    @Override
    public Class<Double> getType() {
        return Double.class;
    }
}
