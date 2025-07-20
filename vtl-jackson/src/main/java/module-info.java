/** This module exposes a Jackson module for the VTL model. */
module fr.insee.vtl.jackson {
  requires java.scripting;
  requires fr.insee.vtl.model;
  requires com.fasterxml.jackson.databind;
  requires org.threeten.extra;

  exports fr.insee.vtl.jackson;
}
