package fr.insee.vtl.engine.functions;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/** Built-in scalar function catalogue for the VTL engine. */
public interface BuiltinFunctionProvider {

  Map<String, List<Method>> getFunctions();
}
