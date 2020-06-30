package fr.insee.vtl.model;

import javax.script.Bindings;
import javax.script.ScriptContext;
import java.util.Map;
import java.util.function.Function;

public interface ResolvableExpression extends TypedExpression {

    static <T> ResolvableExpression withType(Class<T> clazz, Function<ScriptContext, T> func) {
        return new ResolvableExpression() {
            @Override
            public Object resolve(ScriptContext context) {
                return func.apply(context);
            }

            @Override
            public Object resolve(Map<String, Object> context) {
                return null;
            }

            @Override
            public Class<?> getType() {
                return clazz;
            }
        };
    }

    @Deprecated
    Object resolve(ScriptContext context);

    Object resolve(Map<String, Object> context);


}
