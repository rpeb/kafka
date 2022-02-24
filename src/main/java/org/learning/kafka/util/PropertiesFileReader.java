package org.learning.kafka.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class PropertiesFileReader {
    public static java.util.Properties getProperties(String propertiesFile) {
        java.util.Properties props = null;
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(propertiesFile);
            props = new java.util.Properties();
            props.load(fis);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return props;
    }
}
