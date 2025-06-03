package fr.insee.vtl.prov;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.prov.prov.Program;
import org.junit.jupiter.api.Test;

public class ProvenanceListenerTest {

  @Test
  public void simpleTest() {
    String script =
        """
                ds_sum := ds1 + ds2;
                ds_mul := ds_sum * 3;\s
                ds_res <- ds_mul[filter mod(var1, 2) = 0][calc var_sum := var1 + var2];\
                """;

    Program program =
        ProvenanceListener.run(script, "trevas-simple-test", "Simple test from Trevas tests");
    assertThat(program.getProgramSteps()).hasSize(3);
  }

  @Test
  public void testWithEmptyLines() {
    String script =
        """


            ds1 := 'data.ds1'[calc identifier id1 := id1, var1 := cast(var1, integer), var2 := cast(var2, integer)];


            ds2_out := 'other.ds2'[calc identifier id1 := id1, var1 := cast(var1, integer), var2 := cast(var2, integer)];
            ds_sum := ds1 + ds2_out;
            ds_mul <- ds_sum * 3;
            'data.ds_res' <- ds_mul[filter mod(var1, 2) = 0][calc var_sum := var1 + var2];



        """;

    Program program =
        ProvenanceListener.run(script, "trevas-simple-test", "Simple test from Trevas tests");
    assertThat(program.getProgramSteps()).hasSize(5);
  }
}
