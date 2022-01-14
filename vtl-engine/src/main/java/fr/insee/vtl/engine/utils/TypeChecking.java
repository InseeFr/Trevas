package fr.insee.vtl.engine.utils;

import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.TypedExpression;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.Objects;
import java.util.stream.Stream;

/**
 * The <code>TypeChecking</code> class contains useful methods for checking the type of VTL expressions.
 */
public class TypeChecking {

    /**
     * Default constructor overridden to raise an exception: no instance of this class should be created.
     */
    private TypeChecking() {
        throw new IllegalStateException("Type checking utility class");
    }

    /**
     * Asserts that an expression is of a given type.
     * <p>
     * If the expression is null (see {@link #isNull(TypedExpression)}), the type of the returned expression will be the expected type.
     *
     * @param expression The expression to check.
     * @param type       The type to check against.
     * @param tree       The tree of the expression.
     * @return The expression with the given type, even if originally null.
     * @throws VtlRuntimeException with {@link InvalidTypeException} as a cause if the expression is not null and not of the required type.
     */
    public static <T extends TypedExpression> T assertTypeExpression(T expression, Class<?> type, ParseTree tree) {
        if (isNull(expression)) {
            return (T) ResolvableExpression.withType(type, ctx -> null);
        }
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
     * @return A boolean which is <code>true</code> if the expression can be interpreted as the given type, <code>false</code> otherwise.
     */
    public static boolean isType(TypedExpression expression, Class<?> type) {
        return type.isAssignableFrom(expression.getType());
    }

    /**
     * Asserts that an expression is either long or double, or of a given type.
     * <p>
     * If the expression is null (see {@link #isNull(TypedExpression)}), the type of the returned expression will be the expected type.
     *
     * @param expression The expression to check.
     * @param type       The type to check against.
     * @param tree       The tree of the expression.
     * @return The expression with the given type, even if originally null.
     * @throws VtlRuntimeException with {@link InvalidTypeException} as a cause if the expression is not null and not of the required type.
     */
    public static <T extends TypedExpression> T assertNumberOrTypeExpression(T expression, Class<?> type, ParseTree tree) {
        if (!isNumberOrSameType(expression, type)) {
            throw new VtlRuntimeException(new InvalidTypeException(type, expression.getType(), tree));
        }
        return expression;
    }

    /**
     * Checks if expression is either long or double, or of given type.
     *
     * @param expression The expression to check.
     * @param type       The type to check against.
     * @return A boolean which is <code>true</code> if the expression can be interpreted as the given type, <code>false</code> otherwise.
     */
    public static boolean isNumberOrSameType(TypedExpression expression, Class<?> type) {
        var expressionType = expression.getType();
        if (isNumber(expression) && Number.class.isAssignableFrom(type)) return true;
        return type.isAssignableFrom(expressionType);
    }

    /**
     * Checks if expressions have the same type (or null type).
     *
     * @param expressions Resolvable expressions to check.
     * @return A boolean which is <code>true</code> if the expressions have the same type, <code>false</code> otherwise.
     */
    public static boolean hasSameTypeOrNull(ResolvableExpression... expressions) {
        return Stream.of(expressions)
                .map(ResolvableExpression::getType)
                .filter(clazz -> !Object.class.equals(clazz))
                .distinct()
                .count() <= 1;
    }

    /**
     * Checks if an expression evaluates to null.
     * The check is based on the fact that {@link TypedExpression#getType()} returns <code>Object</code> if the expression evaluates to
     * null (otherwise, it returns a more specific class like <code>Boolean</code>).
     *
     * @param expression The expression to check.
     * @return A boolean which is <code>true</code> if the expression evaluates to null, <code>false</code> otherwise.
     */
    public static boolean isNull(TypedExpression expression) {
        return Object.class.equals(expression.getType());
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

    /**
     * Asserts that an expression is of type <code>Number</code>, otherwise raises an exception.
     *
     * @param expression The expression to check.
     * @param tree       The tree of the expression.
     * @param <T>        The class of the expression provided (extends {@link TypedExpression}).
     * @return The expression (typed as number if it evaluates to null).
     */
    public static <T extends TypedExpression> T assertNumber(T expression, ParseTree tree) {
        return assertTypeExpression(expression, Number.class, tree);
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

    /**
     * Asserts that an expression is of type <code>Long</code>, otherwise raises an exception.
     *
     * @param expression The expression to check.
     * @param tree       The tree of the expression.
     * @param <T>        The class of the expression provided (extends {@link TypedExpression}).
     * @return The expression (typed as long integer if it evaluates to null).
     */
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

    /**
     * Asserts that an expression is of type <code>Double</code>, otherwise raises an exception.
     *
     * @param expression The expression to check.
     * @param tree       The tree of the expression.
     * @param <T>        The class of the expression provided (extends {@link TypedExpression}).
     * @return The expression (typed as double-precision number if it evaluates to null).
     */
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

    /**
     * Asserts that an expression is of type <code>Boolean</code>, otherwise raises an exception.
     *
     * @param expression The expression to check.
     * @param tree       The tree of the expression.
     * @param <T>        The class of the expression provided (extends {@link TypedExpression}).
     * @return The expression (typed as boolean if it evaluates to null).
     */
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

    /**
     * Asserts that an expression is of type <code>String</code>, otherwise raises an exception.
     *
     * @param expression The expression to check.
     * @param tree       The tree of the expression.
     * @param <T>        The class of the expression provided (extends {@link TypedExpression}).
     * @return The expression (typed as string if it evaluates to null).
     */
    public static <T extends TypedExpression> T assertString(T expression, ParseTree tree) {
        return assertTypeExpression(expression, String.class, tree);
    }

    /**
     * Checks if a list of objects contains one or more <code>null</code> values.
     *
     * @param objects The objects to check.
     * @return A boolean which is <code>true</code> if one or more objects is <code>null</code>, <code>false</code> otherwise.
     */
    public static boolean hasNullArgs(Object... objects) {
        return Stream.of(objects).anyMatch(Objects::isNull);
    }
}
