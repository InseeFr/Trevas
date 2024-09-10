package fr.insee.vtl.prov;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ProvenanceListenerTest {

    @Test
    public void simpleTest() {
        String script = "ds_sum := ds1 + ds2;\n" +
                "ds_mul := ds_sum * 3; \n" +
                "ds_res <- ds_mul[filter mod(var1, 2) = 0][calc var_sum := var1 + var2];";

        List<Object> obj = ProvenanceListener.parseAndListen(script);
        assertThat(obj).hasSize(4);
    }
}