/**
 * This module expose a Jackson Module for VTL model.
 */

module fr.insee.vtl.jackson {
    requires java.scripting;
    requires fr.insee.vtl.model;
    requires com.fasterxml.jackson.databind;
    exports fr.insee.vtl.jackson;
}