package fr.insee.vtl.engine.utils;

import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.TypedExpression;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.Set;

/**
 * <code>TypeChecking</code> class contains useful methods for checking the type of VTL expressions.
 */
public class TypeChecking {

    private TypeChecking() {
        throw new IllegalStateException("Type checking utility class");
    }

    /**
     * Assert that an expression is of the given type.
     *
     * @param expression the expression to check.
     * @param type       the type to check against.
     * @param tree       the tree of the expression.
     * @return the expression.
     * @throws VtlRuntimeException with {@link InvalidTypeException} as a cause.
     */
    public static <T extends TypedExpression> T assertTypeExpression(T expression, Class<?> type, ParseTree tree) {
        if (!isType(expression, type)) {
            throw new VtlRuntimeException(new InvalidTypeException(type, expression.getType(), tree));
        }
        return expression;
    }

    /**
     * Checks if an expression can be interpreted as a type.
     *
     * @param expression The expression to check.
     * @param type       The type to check against.
     * @return A boolean which is <code>true</code> if the expression can be interpreted as a type, <code>false</code> otherwise.
     */
    public static boolean isType(TypedExpression expression, Class<?> type) {
        return type.isAssignableFrom(expression.getType());
    }

    /**
     * Checks if an expression can be interpreted as a number.
     *
     * @param expression The expression to check.
     * @return A boolean which is <code>true</code> if the expression can be interpreted as a number, <code>false</code> otherwise.
     */
    public static boolean isNumber(TypedExpression expression) {
        return isType(expression, Number.class);
    }

    public static <T extends TypedExpression> T assertNumber(T expression, ParseTree tree) {
        if (!isType(expression, Number.class)) {
            throw new VtlRuntimeException(
                    new InvalidTypeException(Set.of(Long.class, Double.class), expression.getType(), tree)
            );
        }
        return expression;
    }

    /**
     * Checks if an expression can be interpreted as a long integer.
     *
     * @param expression The expression to check.
     * @return A boolean which is <code>true</code> if the expression can be interpreted as a long integer, <code>false</code> otherwise.
     */
    public static boolean isLong(TypedExpression expression) {
        return isType(expression, Long.class);
    }

    public static <T extends TypedExpression> T assertLong(T expression, ParseTree tree) {
        return assertTypeExpression(expression, Long.class, tree);
    }

    /**
     * Checks if an expression can be interpreted as a double-precision number.
     *
     * @param expression The expression to check.
     * @return A boolean which is <code>true</code> if the expression can be interpreted as a double-precision number, <code>false</code> otherwise.
     */
    public static boolean isDouble(TypedExpression expression) {
        return isType(expression, Double.class);
    }

    public static <T extends TypedExpression> T assertDouble(T expression, ParseTree tree) {
        return assertTypeExpression(expression, Double.class, tree);
    }

    /**
     * Checks if an expression can be interpreted as a boolean.
     *
     * @param expression The expression to check.
     * @return A boolean which is <code>true</code> if the expression can be interpreted as a boolean, <code>false</code> otherwise.
     */
    public static boolean isBoolean(TypedExpression expression) {
        return isType(expression, Boolean.class);
    }

    public static <T extends TypedExpression> T assertBoolean(T expression, ParseTree tree) {
        return assertTypeExpression(expression, Boolean.class, tree);
    }

    /**
     * Checks if an expression can be interpreted as a string.
     *
     * @param expression The expression to check.
     * @return A boolean which is <code>true</code> if the expression can be interpreted as a string, <code>false</code> otherwise.
     */
    public static boolean isString(TypedExpression expression) {
        return isType(expression, String.class);
    }

    public static <T extends TypedExpression> T assertString(T expression, ParseTree tree) {
        return assertTypeExpression(expression, String.class, tree);
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
