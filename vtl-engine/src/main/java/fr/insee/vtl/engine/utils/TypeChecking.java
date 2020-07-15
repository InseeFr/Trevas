package fr.insee.vtl.engine.utils;

import fr.insee.vtl.model.TypedExpression;

public class TypeChecking {

    public static boolean isNumber(TypedExpression expression) {
        return Number.class.isAssignableFrom(expression.getType());
    }

    public static boolean isLong(TypedExpression expression) {
        return Long.class.isAssignableFrom(expression.getType());
    }

    public static boolean isDouble(TypedExpression expression) {
        return Double.class.isAssignableFrom(expression.getType());
    }

    public static boolean isDataset(TypedExpression expression) {
        return false;
    }

}
