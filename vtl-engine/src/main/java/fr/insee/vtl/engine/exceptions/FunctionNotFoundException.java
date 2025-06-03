package fr.insee.vtl.engine.exceptions;

import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.exceptions.VtlScriptException;

public class FunctionNotFoundException extends VtlScriptException {

  public FunctionNotFoundException(String funcName, Positioned element) {
    super("function '%s' not found".formatted(funcName), element);
  }
}
