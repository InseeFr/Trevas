package fr.insee.vtl.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The <code>ListExpression</code> class is an abstract representation of an expression of type <code>List</code>.
 */
public abstract class ListExpression implements ResolvableExpression, TypedContainerExpression {

    public static <T> ListExpression withContainedType(Collection<Object> elements, Class<T> containedType) {
        List<Object> list = new ArrayList<>(elements);
        return new ListExpression() {
            @Override
            public List<?> resolve(Map<String, Object> context) {
                return list;
            }

            @Override
            public Class<?> containedType() {
                return containedType;
            }
        };
    }

    @Override
    public Class<?> getType() {
        return List.class;
    }


    @Override
    public abstract List<?> resolve(Map<String, Object> context);
}
