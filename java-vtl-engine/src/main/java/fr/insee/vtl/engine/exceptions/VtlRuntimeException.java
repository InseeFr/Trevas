package fr.insee.vtl.engine.exceptions;

public class VtlRuntimeException extends RuntimeException {
    public VtlRuntimeException(VtlScriptException cause) {
        super(cause);
    }

    @Override
    public VtlScriptException getCause() {
        return (VtlScriptException) super.getCause();
    }
}
