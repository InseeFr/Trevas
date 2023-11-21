import io.sdmx.core.sdmx.manager.structure.SdmxRestToBeanRetrievalManager;
import io.sdmx.fusion.service.constant.REST_API_VERSION;
import io.sdmx.fusion.service.manager.RESTSdmxBeanRetrievalManager;
import org.junit.jupiter.api.Test;

class TestTest {


    // https://github.com/sdmx-twg/sdmx-ml/tree/master/samples
    @Test
    void test() {

        RESTSdmxBeanRetrievalManager manager = new RESTSdmxBeanRetrievalManager("http://example.com", REST_API_VERSION.v1_5_0);
        var beanManager = new SdmxRestToBeanRetrievalManager(manager);
    }
}