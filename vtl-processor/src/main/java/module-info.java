module fr.insee.vtl.processing {

    requires fr.insee.vtl.model;

    exports fr.insee.vtl.processing;

    provides fr.insee.vtl.processing.ProcessingEngine with fr.insee.vtl.processing.InMemoryProcessingEngine;

}