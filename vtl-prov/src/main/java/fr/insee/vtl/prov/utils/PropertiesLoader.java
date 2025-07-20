package fr.insee.vtl.prov.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesLoader {

  public static Properties loadProperties() throws IOException {
    Properties configuration = new Properties();
    InputStream inputStream =
        PropertiesLoader.class.getClassLoader().getResourceAsStream("trevas.properties");
    if (null != inputStream) configuration.load(inputStream);
    InputStream inputStreamDev =
        PropertiesLoader.class.getClassLoader().getResourceAsStream("trevas-dev.properties");
    if (null != inputStreamDev) configuration.load(inputStreamDev);
    inputStream.close();
    return configuration;
  }
}
