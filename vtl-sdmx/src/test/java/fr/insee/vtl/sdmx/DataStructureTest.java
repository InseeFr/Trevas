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
import io.sdmx.utils.core.io.ReadableDataLocationTmp;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

import static fr.insee.vtl.model.Structured.*;

class DataStructureTest {

    private final StaxStructureReaderEngine readerEngine = StaxStructureReaderEngineV3.getInstance();


    private Map<String, DataStructure> parseDataStructure(SdmxBeans sdmxBeans) {
        return sdmxBeans.getDataStructures().stream().collect(Collectors.toMap(
                INamedBean::getId,
                this::convertStructure
        ));
    }

    private DataStructure convertStructure(DataStructureBean sdmxStructure) {
        var components = sdmxStructure.getComponents().stream().map(sdmxComp -> {
            var role = convertTypeToRole(sdmxComp.getType());
            var name = sdmxComp.getId();
            var type = convertType(sdmxComp);
            return new Component(name, type, role);
        }).collect(Collectors.toList());
        return new DataStructure(components);
    }

    private Class<?> convertType(ComponentBean sdmxComp) {
        // TODO: Find the type.
        //sdmxComp.getConceptRef().getSomething?()
        return String.class;
    }

    private Dataset.Role convertTypeToRole(ComponentBean.COMPONENT_TYPE type) {
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

    // https://github.com/sdmx-twg/sdmx-ml/tree/master/samples
    @Test
    void test() throws IOException {
        var structureID = "BPE_CUBE_2021";
        ReadableDataLocation rdl = new ReadableDataLocationTmp("src/test/resources/DSD_BPE_CUBE_2021.xml");
        SdmxBeans sdmxBeans = readerEngine.getSdmxBeans(rdl);

        var structures = parseDataStructure(sdmxBeans);
        var structure = structures.get(structureID);

        var dataset = new CSVDataset(structure, new FileReader("src/test/resources/bpe_cube_2021_sample.csv"));

        dataset.getDataAsMap().forEach(System.out::println);
    }
}
