package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.engine.VtlScriptException;

public class VtlRuntimeException extends RuntimeException {
    public VtlRuntimeException(VtlScriptException cause) {
        super(cause);
    }

    @Override
    public VtlScriptException getCause() {
        return (VtlScriptException) super.getCause();
    }
}
