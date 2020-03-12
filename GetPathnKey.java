/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package configswingdemo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilePermission;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

/**
 *
 * @author root
 */
public class GetPathnKey {
    
    public Properties configProps;
    public File configFile = new File("/usr/lib/flume/apache-flume-1.6.0-bin/conf/flume.conf");
    public FilePermission perm = new java.io.FilePermission("/usr/lib/flume/apache-flume-1.6.0-bin/conf/flume.conf","write");
    
    public String path,key;
    
    public void saveProperties() throws IOException {
        

		configProps.setProperty("TwitterAgent.sources.Twitter.keywords", key);
		configProps.setProperty("TwitterAgent.sinks.HDFS.hdfs.path", path);
		//configProps.setProperty("user", textUser.getText());
		//configProps.setProperty("pass", textPass.getText());
                
		OutputStream outputStream = new FileOutputStream(configFile);
		configProps.store(outputStream, "host setttings");
		
                outputStream.close();
	}
    
            public void loadProperties() throws IOException {
		Properties defaultProps = new Properties();
		// sets default properties
		defaultProps.setProperty("TwitterAgent.sources.Twitter.keywords", "123");
		defaultProps.setProperty("TwitterAgent.sinks.HDFS.hdfs.path", "1111");
		//defaultProps.setProperty("user", "aj");
		//defaultProps.setProperty("pass", "aj");
		
		configProps = new Properties(defaultProps);
		
		// loads properties from file
		InputStream inputStream = new FileInputStream(configFile);
		configProps.load(inputStream);
		inputStream.close();
	}
	
	
}
