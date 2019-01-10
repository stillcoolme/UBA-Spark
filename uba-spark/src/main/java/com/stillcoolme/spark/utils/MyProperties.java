package com.stillcoolme.spark.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author stillcoolme
 * @date: 2018/2/7 15:24
 **/
public class MyProperties extends Properties{

    public static MyProperties getInstance(String location) {
        MyProperties myProperties = new MyProperties();
        if (location != null && location.length() > 0) {
            InputStream inputStream = null;
            if (location.toLowerCase().startsWith("classpath:")) {
                String path = location.substring("classpath:".length());
                if (!path.startsWith("/")) {
                    path = "/" + path;
                }

                inputStream = MyProperties.class.getResourceAsStream(path);
            } else {
                if (!location.startsWith("/")) {
                    location = "/" + location;
                }

                inputStream = MyProperties.class.getResourceAsStream(location);
            }

            try {
                myProperties.load(inputStream);
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            return myProperties;
        } else {
            return null;
        }
    }


}
