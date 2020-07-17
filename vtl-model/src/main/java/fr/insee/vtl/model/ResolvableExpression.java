package fr.insee.vtl.model;

import java.util.Map;
import java.util.function.Function;

public interface ResolvableExpression extends TypedExpression {

    static <T> ResolvableExpression withType(Class<T> clazz, Function<Map<String, Object>, T> func) {
        return new ResolvableExpression() {

            @Override
            public Object resolve(Map<String, Object> context) {
                return func.apply(context);
            }

            @Override
            public Class<?> getType() {
                return clazz;
            }
        };
    }

    Object resolve(Map<String, Object> context);


}
