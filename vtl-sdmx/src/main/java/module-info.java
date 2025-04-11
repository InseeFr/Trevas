/**
 * This module exposes SDMX tools for VTL.
 */

module fr.insee.vtl.sdmx {
    requires fr.insee.vtl.model;
    requires fusion.api.sdmx;
    requires fusion.api;
    requires java.scripting;
    requires fusion.sdmx.ml;
    requires fusion.utils;
    exports fr.insee.vtl.sdmx;
}