package fr.insee.vtl.engine.exceptions;

import org.antlr.v4.runtime.tree.ParseTree;

import java.util.List;
import java.util.stream.Collectors;

public class InvalidTypeException extends VtlScriptException {

    private final Class<?> expectedType;
    private final List<Class<?>> expectedTypes;
    private final Class<?> receivedType;

    public InvalidTypeException(ParseTree tree, Class<?> expectedType, Class<?> receivedType) {
        super(String.format("invalid type %s, expected %s to be %s",
                receivedType.getSimpleName(), tree.getText(), expectedType.getSimpleName()
        ), tree);
        this.expectedType = expectedType;
        this.expectedTypes = null;
        this.receivedType = receivedType;
    }

    public InvalidTypeException(ParseTree tree, List<Class<?>> expectedTypes, Class<?> receivedType) {
        super(String.format("invalid type %s, expected %s to be %s",
                receivedType.getSimpleName(), tree.getText(),
                expectedTypes
                        .stream()
                        .map(Class::getSimpleName)
                .collect(Collectors.joining(" or "))
        ), tree);
        this.expectedType = null;
        this.expectedTypes = expectedTypes;
        this.receivedType = receivedType;
    }

    public Class<?> getExpectedType() {
        return expectedType;
    }

    public List<Class<?>> getExpectedTypes() {
        return expectedTypes;
    }

    public Class<?> getReceivedType() {
        return receivedType;
    }
}
