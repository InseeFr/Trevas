package fr.insee.vtl.engine.utils;

import fr.insee.vtl.model.TypedExpression;

/**
 * <code>TypeChecking</code> class contains useful methods for checking the type of VTL expressions.
 */
public class TypeChecking {

    private TypeChecking() {
        throw new IllegalStateException("Type checking utility class");
    }

    /**
     * Checks if an expression can be interpreted as a number.
     *
     * @param expression The expression to check.
     * @return A boolean which is <code>true</code> if the expression can be interpreted as a number, <code>false</code> otherwise.
     */
    public static boolean isNumber(TypedExpression expression) {
        return Number.class.isAssignableFrom(expression.getType());
    }

    /**
     * Checks if an expression can be interpreted as a long integer.
     *
     * @param expression The expression to check.
     * @return A boolean which is <code>true</code> if the expression can be interpreted as a long integer, <code>false</code> otherwise.
     */
    public static boolean isLong(TypedExpression expression) {
        return Long.class.isAssignableFrom(expression.getType());
    }

    /**
     * Checks if an expression can be interpreted as a double-precision number.
     *
     * @param expression The expression to check.
     * @return A boolean which is <code>true</code> if the expression can be interpreted as a double-precision number, <code>false</code> otherwise.
     */
    public static boolean isDouble(TypedExpression expression) {
        return Double.class.isAssignableFrom(expression.getType());
    }

    /**
     * Checks if an expression can be interpreted as a boolean.
     *
     * @param expression The expression to check.
     * @return A boolean which is <code>true</code> if the expression can be interpreted as a boolean, <code>false</code> otherwise.
     */
    public static boolean isBoolean(TypedExpression expression) {
        return Boolean.class.isAssignableFrom(expression.getType());
    }

    /**
     * Checks if an expression can be interpreted as a dataset.
     *
     * @param expression The expression to check.
     * @return A boolean which is <code>true</code> if the expression can be interpreted as a dataset, <code>false</code> otherwise.
     */
    public static boolean isDataset(TypedExpression expression) {
        return false;
    }
}
