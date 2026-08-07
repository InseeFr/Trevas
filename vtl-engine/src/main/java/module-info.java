import fr.insee.vtl.engine.VtlScriptEngineFactory;
import fr.insee.vtl.engine.processors.InMemoryProcessingEngine;
import fr.insee.vtl.model.FunctionProvider;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ProcessingEngineFactory;
import javax.script.ScriptEngineFactory;

/** This module contains the actual VTL engine. */
module fr.insee.vtl.engine {
  exports fr.insee.vtl.engine.exceptions;
  exports fr.insee.vtl.engine.processors;

  requires transitive java.scripting;
  requires transitive fr.insee.vtl.parser;
  requires transitive fr.insee.vtl.model;
  requires org.apache.commons.lang3;
  requires org.apache.commons.text;
  requires safety.mirror;
  requires org.threeten.extra;
  requires org.jgrapht.core;

  uses ProcessingEngine;
  uses ProcessingEngineFactory;
  uses FunctionProvider;

  provides ProcessingEngineFactory with
      InMemoryProcessingEngine.Factory;
  provides ScriptEngineFactory with
      VtlScriptEngineFactory;

  // Fun.toMethod (safety-mirror) reflects into method-reference lambdas in these packages.
  // `opens fr.insee.vtl.engine` does not open subpackages.
  opens fr.insee.vtl.engine;
  opens fr.insee.vtl.engine.functions.providers to safety.mirror;
}
