package fr.insee.vtl.model;

import javax.script.ScriptContext;
import java.util.function.Function;

public abstract class LongExpression extends NumberExpression {

    private LongExpression() {
    }

    public static LongExpression withFunction(Function<ScriptContext, Long> func) {
        return new LongExpression() {
            @Override
            public Long resolve(ScriptContext context) {
                return func.apply(context);
            }
        };
    }

    @Override
    public abstract Long resolve(ScriptContext context);

    @Override
    public Class<Long> getType() {
        return Long.class;
    }
}