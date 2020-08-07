package fr.insee.vtl.engine.exceptions;

import org.antlr.v4.runtime.tree.ParseTree;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The <code>InvalidTypeException</code> indicates that an element used in an expression has a type which is incompatible with this expression.
 */
public class InvalidTypeException extends VtlScriptException {

    private final Class<?> expectedType;
    private final Set<Class<?>> expectedTypes;
    private final Class<?> receivedType;

    /**
     * Constructor taking the parsing context, the expected type and the actual type encountered.
     *
     * @param tree The parsing context where the exception is thrown.
     * @param expectedType The type supported in the context.
     * @param receivedType The type actually encountered.
     */
    public InvalidTypeException(Class<?> expectedType, Class<?> receivedType, ParseTree tree) {
        super(String.format("invalid type %s, expected %s to be %s",
                receivedType.getSimpleName(), tree.getText(), expectedType.getSimpleName()
        ), tree);
        this.expectedType = expectedType;
        this.expectedTypes = Set.of(expectedType);
        this.receivedType = receivedType;
    }

    /**
     * Constructor taking the parsing context, a list of expected types and the actual type encountered.
     *  @param tree The parsing context where the exception is thrown.
     * @param expectedTypes The list of types supported in the context.
     * @param receivedType The type actually encountered.
     */
    public InvalidTypeException(Set<Class<?>> expectedTypes, Class<?> receivedType, ParseTree tree) {
        super(String.format("invalid type %s, expected %s to be %s",
                receivedType.getSimpleName(), tree.getText(),
                expectedTypes
                        .stream()
                        .map(Class::getSimpleName)
                        .sorted()
                .collect(Collectors.joining(" or "))
        ), tree);
        this.expectedType = null;
        this.expectedTypes = expectedTypes;
        this.receivedType = receivedType;
    }

    /**
     * Returns the type which was expected when the exception was thrown.
     *
     * @return The type which was expected when the exception was thrown.
     */
    public Class<?> getExpectedType() {
        return expectedType;
    }

    /**
     * Returns the types which were expected when the exception was thrown.
     *
     * @return The types which were expected when the exception was thrown.
     */
    public Set<Class<?>> getExpectedTypes() {
        return expectedTypes;
    }

    /**
     * Returns the type which was actually received and caused the exception to be thrown.
     *
     * @return The types type which was actually received and caused the exception to be thrown.
     */
    public Class<?> getReceivedType() {
        return receivedType;
    }
}
