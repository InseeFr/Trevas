import fr.insee.vtl.engine.VtlScriptEngineFactory;
import fr.insee.vtl.engine.functions.LevenshteinProvider;
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

  provides FunctionProvider with
      LevenshteinProvider;
  provides ProcessingEngineFactory with
      InMemoryProcessingEngine.Factory;
  provides ScriptEngineFactory with
      VtlScriptEngineFactory;

  opens fr.insee.vtl.engine;
}
