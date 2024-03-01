import io.sdmx.api.io.ReadableDataLocation;
import io.sdmx.api.sdmx.model.beans.SdmxBeans;
import io.sdmx.format.ml.api.engine.StaxStructureReaderEngine;
import io.sdmx.format.ml.engine.structure.reader.v3.StaxStructureReaderEngineV3;
import io.sdmx.utils.core.io.ReadableDataLocationTmp;
import org.junit.jupiter.api.Test;

class DataStructureTest {

    private final StaxStructureReaderEngine readerEngine = StaxStructureReaderEngineV3.getInstance();

    // https://github.com/sdmx-twg/sdmx-ml/tree/master/samples
    @Test
    void test() {
        ReadableDataLocation rdl = new ReadableDataLocationTmp("src/test/resources/DSD_BPE_CUBE_2021.xml");
        SdmxBeans sdmxBeans = readerEngine.getSdmxBeans(rdl);
        sdmxBeans.getDataStructures().forEach(ds -> {
            ds.getComponents().forEach(c -> {
                System.out.print(c.getId() + " - ");
                System.out.print(c.getType());
            });
        });
    }
}