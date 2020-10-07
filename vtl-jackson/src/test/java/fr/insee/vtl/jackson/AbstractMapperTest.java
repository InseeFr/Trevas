package fr.insee.vtl.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;

public class AbstractMapperTest {

    protected ObjectMapper mapper;

    @BeforeEach
    public void setUp() throws IOException {
        mapper = new ObjectMapper();
        mapper.registerModule(new TrevasModule());
    }
}
