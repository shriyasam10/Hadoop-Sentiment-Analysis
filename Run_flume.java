/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package run_flume;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

/**
 *
 * @author aj
 */
public class Run_flume {
    
    
public void run1() throws SQLException, IOException {  
    
    Runtime.getRuntime().exec("/usr/bin/xterm");
    
    
    
}  



/*

    public static void main(String[] args) throws IOException, InterruptedException {
        
        Run_flume r= new Run_flume();
    }
*/
    
}

        /*
        String cmd ="/usr/lib/flume/apache-flume-1.6.0-bin/bin/./flume-ng agent -n TwitterAgent -c conf -f /usr/lib/flume/apache-flume-1.6.0-bin/conf/flume.conf";
        Process proc=Runtime.getRuntime().exec(cmd);
       
        proc.waitFor();
        */
       
        /*
        String[] args = new String[] {"/usr/lib/flume/apache-flume-1.6.0-bin/bin/./flume-ng agent -n TwitterAgent -c conf -f /usr/lib/flume/apache-flume-1.6.0-bin/conf/flume.conf"};
        Process proc = new ProcessBuilder(args).start();
        */
        
        /*
         String[] command = {"./flume-ng agent", "-n", "TwitterAgent", "-c", "conf", "-f", "flume.conf"};
                        ProcessBuilder builder = new ProcessBuilder(command);
                        builder.directory(new File ("/usr/lib/flume/apache-flume-1.6.0-bin/bin/"));
                        Process p = builder.start();
        */
        
     /*   
    String[] args1;
        args1 = new String[] { "/usr/lib/flume/apache-flume-1.6.0-bin/bin/./flume-ng ", "-nTwitterAgent", "-fflume.conf" };
        Process proc=Runtime.getRuntime().exec(args1);
    */
        
    
   /* Where "Agent" is the name of your Flume Agent.
"flume.conf" is the configuration file which should be placed in the resources folder of your Java project.
    */

   
   
   
   /*
   Runtime r = Runtime.getRuntime();
   String myScript ="/usr/lib/flume/apache-flume-1.6.0-bin/bin/./flume-ng \", \"-nTwitterAgent\", \"-fflume.conf";
   String[] cmdArray ={"","", myScript+"; le_exec" };
   
    */
   
   //Runtime.getRuntime().exec("sh /home/aj/abc.sh");
   
    
    

    

