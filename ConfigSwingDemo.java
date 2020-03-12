/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package configswingdemo;

/**
 *
 * @author aj
 */
//package net.codejava.swing;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilePermission;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import static java.util.stream.DoubleStream.builder;
//import java.util.ArrayList;
//import java.util.List;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;



public class ConfigSwingDemo extends JFrame {
	private File configFile = new File("/usr/lib/flume/apache-flume-1.6.0-bin/conf/flume.conf");
	private Properties configProps;
	
        FilePermission perm = new java.io.FilePermission("/usr/lib/flume/apache-flume-1.6.0-bin/conf/flume.conf","write");
        Process p;
	private JLabel labelHost = new JLabel("TwitterAgent.sources.Twitter.keywords=");
	private JLabel labelPort = new JLabel("TwitterAgent.sinks.HDFS.hdfs.path=");
	private JLabel labelUser = new JLabel("Username: ");
	private JLabel labelPass = new JLabel("Password: ");
	
	private JTextField textHost = new JTextField(20);
	private JTextField textPort = new JTextField(20);
	private JTextField textUser = new JTextField(20);
	private JTextField textPass = new JTextField(20);
	
	private JButton buttonSave = new JButton("Save");
        private JButton buttonDestroy = new JButton("Destroy");
	
	
        
        public ConfigSwingDemo() {
		super("Properties Configuration Demo");
		setLayout(new GridBagLayout());
		GridBagConstraints constraints = new GridBagConstraints();
		constraints.gridx = 0;
		constraints.gridy = 0;
		constraints.insets = new Insets(10, 10, 5, 10);
		constraints.anchor = GridBagConstraints.WEST;
		
		add(labelHost, constraints);
		
		constraints.gridx = 1;
		add(textHost, constraints);
		
		constraints.gridy = 1;
		constraints.gridx = 0;
		add(labelPort, constraints);
		
		constraints.gridx = 1;
		add(textPort, constraints);

		constraints.gridy = 2;
		constraints.gridx = 0;
		add(labelUser, constraints);
		
		constraints.gridx = 1;
		add(textUser, constraints);

		constraints.gridy = 3;
		constraints.gridx = 0;
		add(labelPass, constraints);
		
		constraints.gridx = 1;
		add(textPass, constraints);
		
		constraints.gridy = 4;
		constraints.gridx = 0;
		constraints.gridwidth = 2;
		constraints.anchor = GridBagConstraints.CENTER;
		add(buttonSave, constraints);
                add(buttonDestroy);
		
		
                buttonDestroy.addActionListener(new ActionListener() {
                   

                    @Override
                    public void actionPerformed(ActionEvent ae) {
                         p.destroy();
                        
                    }
                    
                });
                
                buttonSave.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent arg0) {
				try {
					saveProperties();
					JOptionPane.showMessageDialog(ConfigSwingDemo.this, 
							"Properties were saved successfully!");	
                                               //String cmd = "sh /home/aj/abc.sh";
                                               String cmd = "/usr/lib/flume/apache-flume-1.6.0-bin/bin/./flume-ng agent -n TwitterAgent -c conf -f /usr/lib/flume/apache-flume-1.6.0-bin/conf/flume.conf";
                                             Runtime rt=Runtime.getRuntime();{
                        
                                                 try {
                                                    p=rt.exec(cmd);
                                                    int abc = p.waitFor();
                                                     
                                                     
                                                 JOptionPane.showMessageDialog(ConfigSwingDemo.this, 
							"Downloading Started!");	
                                            
                                                 }
                           
                        catch (IOException e) {
                            System.err.println("Caught IOException: " + e.getMessage());
                            JOptionPane.showMessageDialog(ConfigSwingDemo.this, 
							"Downloading not started!");	
                                        
                                               } catch (InterruptedException ex) {
                                                Logger.getLogger(ConfigSwingDemo.class.getName()).log(Level.SEVERE, null, ex);
                                            }
                                        
                
                }
				} catch (IOException ex) {
					JOptionPane.showMessageDialog(ConfigSwingDemo.this, 
							"Error saving properties file: " + ex.getMessage());		
				}
			}
		});
		
                
                
                
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		pack();
		setLocationRelativeTo(null);
		setVisible(true);
		
		try {
			loadProperties();
		} catch (IOException ex) {
			JOptionPane.showMessageDialog(this, "The config.properties file does not exist, default properties loaded.");
		}
		textHost.setText(configProps.getProperty("TwitterAgent.sources.Twitter.keywords"));
		textPort.setText(configProps.getProperty("TwitterAgent.sinks.HDFS.hdfs.path"));
		textUser.setText(configProps.getProperty("user"));
		textPass.setText(configProps.getProperty("pass"));
	}

	
        
        
        
        private void loadProperties() throws IOException {
		Properties defaultProps = new Properties();
		// sets default properties
		defaultProps.setProperty("TwitterAgent.sources.Twitter.keywords", "123");
		defaultProps.setProperty("TwitterAgent.sinks.HDFS.hdfs.path", "1111");
		defaultProps.setProperty("user", "aj");
		defaultProps.setProperty("pass", "aj");
		
		configProps = new Properties(defaultProps);
		
		// loads properties from file
		InputStream inputStream = new FileInputStream(configFile);
		configProps.load(inputStream);
		inputStream.close();
	}
	
	
        
        
        private void saveProperties() throws IOException {
		configProps.setProperty("TwitterAgent.sources.Twitter.keywords", textHost.getText());
		configProps.setProperty("TwitterAgent.sinks.HDFS.hdfs.path", textPort.getText());
		//configProps.setProperty("user", textUser.getText());
		//configProps.setProperty("pass", textPass.getText());
                
		OutputStream outputStream = new FileOutputStream(configFile);
		configProps.store(outputStream, "host setttings");
		outputStream.close();
	}
	
	public static void main(String[] args) throws IOException {
		SwingUtilities.invokeLater(new Runnable() {
			@Override
			public void run() {
				new ConfigSwingDemo();
                               
                        }
                        
                       
                 
                   
                        /*
                        String[] command = {"./flume-ng agent -n TwitterAgent -c conf -f ../conf/flume.conf",""};
                        ProcessBuilder builder = new ProcessBuilder(command);
                        builder.directory(new File (/usr/lib/flume/apache-flume-1.6.0-bin/bin));
                        Process p = builder.start();
*/
                       
                        
		});
	}
}
