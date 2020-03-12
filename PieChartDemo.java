package combi;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.*;
public class PieChartDemo
{
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    
    int count=0;
    public String country[];
    public String sentimentcount[];
    public int count1,count2,count3,count4,count5;
    public String country1,country2,country3,country4,country5;
    public int country1positivesentiment=0,country1negativesentiment=0,country1neutralsentiment=0,country2positivesentiment=0,country2negativesentiment=0,country2neutralsentiment=0;
    public int country3positivesentiment=0,country3negativesentiment=0,country3neutralsentiment=0,country4positivesentiment=0,country4negativesentiment=0,country4neutralsentiment=0,country5positivesentiment=0,country5negativesentiment=0,country5neutralsentiment=0;
    
  public void test()throws Exception
  {
      String driverName = "org.apache.hive.jdbc.HiveDriver";
                Class cls = Class.forName(driverName);
                ClassLoader cLoader = cls.getClassLoader();
                Connection conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hduser", "aj");
		     // wait(10000);
                System.out.println("Connected");
                
                
                
                System.out.println("Creating statement...");
			Statement stmt = conn.createStatement();
                
              
                        
                        
                        
    
    
    String q="select * from tweetsbi";            
    ResultSet res = stmt.executeQuery(q);
    while (res.next()) 
    {      
        count++;
        //System.out.println(res.getString(2) + "\t" + res.getInt(3));
        
    }
    
    
    String sql = "create table if not exists country_count as select country,count(country) from tweetsbi1 group by country";
    System.out.println("Running: " + sql);
    stmt.execute(sql);
    System.out.println("Getting sentiments \n");
    
     sql = "select *from country_count"; 
     res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1) + "\t" + res.getString(2));
    }
   
      country=new String[count];
      sentimentcount=new String[count];
      //System.out.println(""+count);
      
    int i=0;
    res = stmt.executeQuery(sql);
    while (res.next()) 
    {    
        
        country[i]=res.getString(1);
        sentimentcount[i]=res.getString(2);
        i++;
    } 

    
     country1=country[1];    
     country2=country[2];
     country3=country[3];
     country4=country[4];
     country5=country[5];
    
     count1=Integer.parseInt(sentimentcount[1]);
     count2=Integer.parseInt(sentimentcount[2]);
     count3=Integer.parseInt(sentimentcount[3]);
     count4=Integer.parseInt(sentimentcount[4]);
     count5=Integer.parseInt(sentimentcount[5]);
  
     
     String q2="select sentiment from tweetsbi where country='"+country1+"'";
    
    res = stmt.executeQuery(q2);
    
    
    
    while (res.next()) 
    {
        int sentiment=res.getInt(1);
        //System.out.println(""+sentiment);
        if(sentiment==1)
        {
            country1positivesentiment++;
        }
        else if(sentiment==-1)
        {
            country1negativesentiment++;
        }
        else if(sentiment==0)
        {
            country1neutralsentiment++;
        }
        
    }
    
    System.out.println("positive country1:-"+country1positivesentiment);
    System.out.println("negative country1:-"+country1negativesentiment);
    System.out.println("neutral country1:-"+country1neutralsentiment);
    
    
     //second country..chart 3
    
    String q3="select sentiment from tweetsbi where country='"+country2+"'";
    
    res = stmt.executeQuery(q3);
    while (res.next()) 
    {
        int sentiment=res.getInt(1);
       // System.out.println(""+sentiment);
        if(sentiment==1)
        {
            country2positivesentiment++;
        }
        else if(sentiment==-1)
        {
            country2negativesentiment++;
        }
        else if(sentiment==0)
        {
            country2neutralsentiment++;
        }
    }
    
    System.out.println("positive country2:-"+country2positivesentiment);
    System.out.println("negative country2:-"+country2negativesentiment);
    System.out.println("neutral country2:-"+country2neutralsentiment);
   
    String q4="select sentiment from tweetsbi where country='"+country3+"'";
    
    res = stmt.executeQuery(q4);
    while (res.next()) 
    {
        int sentiment=res.getInt(1);
        //System.out.println(""+sentiment);
        if(sentiment==1)
        {
            country3positivesentiment++;
        }
        else if(sentiment==-1)
        {
            country3negativesentiment++;
        }
        else if(sentiment==0)
        {
            country3neutralsentiment++;
        }
    }
    
    System.out.println("positive country3:-"+country3positivesentiment);
    System.out.println("negative country3:-"+country3negativesentiment);
    System.out.println("neutral country3:-"+country3neutralsentiment);
    
     String q5="select sentiment from tweetsbi where country='"+country4+"'";
    
    res = stmt.executeQuery(q5);
    while (res.next()) 
    {
        int sentiment=res.getInt(1);
       if(sentiment==1)
        {
            country4positivesentiment++;
        }
        else if(sentiment==-1)
        {
            country4negativesentiment++;
        }
        else if(sentiment==0)
        {
            country4neutralsentiment++;
        }
    }
    
    System.out.println("positive country4:-"+country4positivesentiment);
    System.out.println("negative country4:-"+country4negativesentiment);
    System.out.println("neutral country4:-"+country4neutralsentiment);
    
   String q6="select sentiment from tweetsbi where country='"+country5+"'";
    
    res = stmt.executeQuery(q6);
    while (res.next()) 
    {
        int sentiment=res.getInt(1);
        if(sentiment==1)
        {
            country5positivesentiment++;
        }
        else if(sentiment==-1)
        {
            country5negativesentiment++;
        }
        else if(sentiment==0)
        {
            country5neutralsentiment++;
        }
    }
    
    System.out.println("positive country5:-"+country5positivesentiment);
    System.out.println("negative country5:-"+country5negativesentiment);
    System.out.println("neutral country5:-"+country5neutralsentiment);

   /* String sql = "select count(country) from tweetsbi group by country";
    System.out.println("Running: " + sql);
   // stmt.execute(sql);
   
    res = stmt.executeQuery(sql);
    System.out.println(res.getString(1) + "\t" + res.getInt(2));
    */
   
    /*
      country=new String[count];
      sentiment=new int[count];
      System.out.println(""+count);
      
    int i=0;
    res = stmt.executeQuery(q);
    while (res.next()) 
    {      
        country[i]=res.getString(2);
        sentiment[i]=res.getInt(3);
        i++;
    } 

    
    //logic starts here(after storing in arrays)
    
    
    System.out.println(country[0]);
*/    
    
    
  }
    
    public static void main(String args[])
    {
        PieChartDemo obj=new PieChartDemo();
        try
        {
        obj.test();
        }catch(Exception e){
        System.out.println(e.getMessage());
        }
    }
}