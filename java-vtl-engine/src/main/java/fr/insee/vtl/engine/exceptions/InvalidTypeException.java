package fr.insee.vtl.engine.exceptions;

import org.antlr.v4.runtime.tree.ParseTree;

public class InvalidTypeException extends VtlScriptException {

    private final Class<?> expectedType;
    private final Class<?> receivedType;

    public InvalidTypeException(ParseTree tree, Class<?> expectedType, Class<?> receivedType) {
        super(String.format("invalid type %s, expected %s to be %s",
                receivedType, tree.getText(), expectedType
        ), tree);
        this.expectedType = expectedType;
        this.receivedType = receivedType;
    }

    public Class<?> getExpectedType() {
        return expectedType;
    }

    public Class<?> getReceivedType() {
        return receivedType;
    }
}
