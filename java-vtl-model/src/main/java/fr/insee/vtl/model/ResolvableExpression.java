package fr.insee.vtl.model;

import javax.script.ScriptContext;
import java.util.function.Function;

public interface ResolvableExpression extends TypedExpression {

    static <T> ResolvableExpression withType(Class<T> clazz, Function<ScriptContext, T> func) {
        return new ResolvableExpression() {
            @Override
            public Object resolve(ScriptContext context) {
                return func.apply(context);
            }

            @Override
            public Class<?> getType(ScriptContext context) {
                return clazz;
            }
        };
    }

    Object resolve(ScriptContext context);

}
