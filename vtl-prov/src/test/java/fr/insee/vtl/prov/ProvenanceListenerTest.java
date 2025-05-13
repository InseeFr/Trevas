package fr.insee.vtl.prov;

import fr.insee.vtl.prov.prov.Program;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProvenanceListenerTest {

    @Test
    public void simpleTest() {
        String script = """
                ds_sum := ds1 + ds2;
                ds_mul := ds_sum * 3;\s
                ds_res <- ds_mul[filter mod(var1, 2) = 0][calc var_sum := var1 + var2];\
                """;

        Program program = ProvenanceListener.run(script, "trevas-simple-test", "Simple test from Trevas tests");
        assertThat(program.getProgramSteps()).hasSize(3);
    }
}