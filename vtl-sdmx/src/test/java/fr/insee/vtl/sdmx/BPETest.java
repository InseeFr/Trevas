package fr.insee.vtl.sdmx;

import fr.insee.vtl.csv.CSVDataset;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Structured;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.FileReader;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class BPETest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void buildDataset() throws IOException, ScriptException {
        Utils u = new Utils();
        Structured.DataStructure structure = u.buildStructureFromSDMX3("src/test/resources/DSD_BPE_DETAIL.xml", "BPE_CUBE_2021");

        Dataset bpeDetailDs = new CSVDataset(structure, new FileReader("src/test/resources/BPE_DETAIL_SAMPLE.csv"));

        ScriptContext context = engine.getContext();
        context.setAttribute("BPE_DETAIL", bpeDetailDs, ScriptContext.GLOBAL_SCOPE);
        engine.eval("ds := BPE_DETAIL;");
        Dataset ds = (Dataset) engine.getContext().getAttribute("BPE_DETAIL");
        assertThat(ds.getDataStructure().size()).isEqualTo(6);
    }
}
