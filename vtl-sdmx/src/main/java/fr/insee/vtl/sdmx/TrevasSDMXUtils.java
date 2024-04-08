package fr.insee.vtl.sdmx;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import io.sdmx.api.io.ReadableDataLocation;
import io.sdmx.api.sdmx.model.beans.SdmxBeans;
import io.sdmx.api.sdmx.model.beans.base.ComponentBean;
import io.sdmx.api.sdmx.model.beans.base.INamedBean;
import io.sdmx.api.sdmx.model.beans.datastructure.DataStructureBean;
import io.sdmx.format.ml.api.engine.StaxStructureReaderEngine;
import io.sdmx.format.ml.engine.structure.reader.v3.StaxStructureReaderEngineV3;
import io.sdmx.utils.core.io.InMemoryReadableDataLocation;
import io.sdmx.utils.core.io.ReadableDataLocationTmp;

import java.io.InputStream;
import java.util.Map;
import java.util.stream.Collectors;

import static fr.insee.vtl.model.Structured.Component;

class TrevasSDMXUtils {

    private static final StaxStructureReaderEngine readerEngineSDMX3 = StaxStructureReaderEngineV3.getInstance();


    private static Map<String, Structured.DataStructure> parseDataStructure(SdmxBeans sdmxBeans) {
        return sdmxBeans.getDataStructures().stream().collect(Collectors.toMap(
                INamedBean::getId,
                TrevasSDMXUtils::convertStructure
        ));
    }

    private static Structured.DataStructure convertStructure(DataStructureBean sdmxStructure) {
        var components = sdmxStructure.getComponents().stream().map(sdmxComp -> {
            var role = convertTypeToRole(sdmxComp.getType());
            var name = sdmxComp.getId();
            var type = convertType(sdmxComp);
            return new Component(name, type, role);
        }).collect(Collectors.toList());
        return new Structured.DataStructure(components);
    }

    private static Class<?> convertType(ComponentBean sdmxComp) {
        // TODO: Find the type.
        //sdmxComp.getConceptRef().getSomething?()
        return String.class;
    }

    private static Dataset.Role convertTypeToRole(ComponentBean.COMPONENT_TYPE type) {
        switch (type) {
            case MEASURE:
                return Dataset.Role.MEASURE;
            case ATTRIBUTE:
            case METADATA_ATTRIBUE:
                return Dataset.Role.ATTRIBUTE;
            case DIMENSION:
                return Dataset.Role.IDENTIFIER;
            default:
                throw new UnsupportedOperationException("unsupported role " + type);
        }
    }

    static private Structured.DataStructure buildStructureFromSDMX3(ReadableDataLocation rdl, String structureID) {
        SdmxBeans sdmxBeans = readerEngineSDMX3.getSdmxBeans(rdl);
        var structures = parseDataStructure(sdmxBeans);
        return structures.get(structureID);
    }

    static public Structured.DataStructure buildStructureFromSDMX3(String sdmxDSDPath, String structureID) {
        ReadableDataLocation rdl = new ReadableDataLocationTmp(sdmxDSDPath);
        return buildStructureFromSDMX3(rdl, structureID);
    }

    static public Structured.DataStructure buildStructureFromSDMX3(InputStream sdmxDSD, String structureID) {
        ReadableDataLocation rdl = new InMemoryReadableDataLocation(sdmxDSD);
        return buildStructureFromSDMX3(rdl, structureID);
    }
}
