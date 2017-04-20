package com.ning.dw.tools;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;



/**
 * 读取配置文件工具
 * 
 * @author ningyexin
 *
 */
public class ConfigReader {

    private static Properties property;

    // private static final String CONF =
    // ConfigReader.class.getClassLoader().getResource("/").getPath();

    static {
        if (null == property) {
            InputStream in = null;
            property = new Properties();
            try {

                in = new FileInputStream("conf.properties");

            } catch (IOException e) {
                try {
                    in = new FileInputStream(ConfigReader.class.getClassLoader().getResource("/").getPath() + "conf.properties");
                } catch (Exception e1) {
                    try {
                        in = new FileInputStream(ConfigReader.class.getClass().getResource("/").getPath() + "conf.properties");
                    } catch (IOException e2) {

                    }
                }
            }

            try {
                property.load(in);
            } catch (IOException e) {
            }

        }
    }

    public static String getPropValue(String key) {
        return property.getProperty(key);
    }

}
