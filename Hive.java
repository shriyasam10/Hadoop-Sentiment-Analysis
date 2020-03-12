/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package combi;


/**
 *
 * @author aj
 */
//public class Hive 
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.*;

public class Hive {
	
     private static String driverName = "org.apache.hive.jdbc.HiveDriver";

     public void hive1() throws SQLException {
    
    try{
                
                String driverName = "org.apache.hive.jdbc.HiveDriver";
                Class cls = Class.forName(driverName);
                ClassLoader cLoader = cls.getClassLoader();
                Connection conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hduser", "aj");
		     // wait(10000);
                System.out.println("Connected");
                
                System.out.println("Creating statement...");
			Statement stmt = conn.createStatement();
                        
                 stmt.execute("add jar /home/aj/A/json-serde-1.3.6-SNAPSHOT-jar-with-dependencies.jar");
                 stmt.execute("add jar /home/aj/A/hive-serdes-1.0-SNAPSHOT.jar");        
                   
          //create database 
         // stmt.execute("CREATE DATABASE db1");
          //System.out.println("Database created");
          stmt.execute("use db1");
          
                        
                        /*
          //create table
               
          stmt.execute("CREATE TABLE IF NOT EXISTS "
         +" student5 (name string, age int, "
         +" mob bigint, city string )"
         +" ROW FORMAT DELIMITED  FIELDS TERMINATED BY ','"
         +" LINES TERMINATED BY '\n'"
         +" STORED AS TEXTFILE");
                 System.out.println("Table created successfully");
                 
               
                 stmt = conn.createStatement(
                           ResultSet.TYPE_SCROLL_INSENSITIVE,
                           ResultSet.CONCUR_READ_ONLY);
              
                 //load data
              stmt.execute("load data local inpath '/home/aj/A/test3.txt' overwrite into table student5");
               System.out.println("Load Data into employee successful");
               
               
       
               
      
              
    // describe tables
    String sql = "describe student5";
    ResultSet res = stmt.executeQuery(sql);
    while (res.next()) 
    {      
        System.out.println(res.getString(1) + "\t" + res.getString(2));
    }
    
     sql = "select * from student5";
    // sql = "select * from " + tableName;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1));
      // + "\t" + res.getString(2)
    }
    
     // regular hive query
    sql = "select count(1) from student5";
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1));
    }
    
     // regular hive query
    sql = "select name from student5";
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1) );
    }
   
        */
                
                        stmt.execute("set hive.support.sql11.reserved.keywords=false");
                  System.out.println("Reserved keywords command executed");
                 
          stmt.execute("CREATE TABLE IF NOT EXISTS "
         +" tweets5 ( id BIGINT, created_at STRING, "
         +"source STRING, "
         +"favorited BOOLEAN, "
         +" retweet_count INT,"
         +"retweeted_status STRUCT< "
         +"text:STRING, "
         +" user:STRUCT<screen_name:STRING,name:STRING>,"
         +"retweet_count:INT>, "
         +" entities STRUCT< "
         +"urls:ARRAY<STRUCT<expanded_url:STRING>>, "
         +"user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>, "
         +" hashtags:ARRAY<STRUCT<text:STRING>>>,"
         +" text STRING,"
         +" user STRUCT<"
         +"screen_name:STRING, "
          +"name:STRING, "
          +"locations:STRING, "
           +"friends_count:INT, " 
           +" followers_count:INT,"
           +"statuses_count:INT, "
           +"verified:BOOLEAN, "
           +"utc_offset:INT, "
           +"time_zone:STRING>, "
         + " in_reply_to_screen_name STRING)"
         +" ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'"
         +" LOCATION '/user/flume/tweets'");
         System.out.println("Table created successfully");
                 
                  
                 
                 
            //load data
             // stmt.execute("load data local inpath '/home/aj/A/test4.txt' overwrite into table tweets1");
             //  System.out.println("Load Data into employee successful"); 
                 
              
            
     // describe tables
    String sql = "describe tweets5";
    ResultSet res = stmt.executeQuery(sql);
    while (res.next()) 
    {      
        System.out.println(res.getString(1) + "\t" + res.getString(2));
    }
    
  
    //regular query
    sql = "select id,user.name from tweets5";
    // sql = "select * from " + tableName;
    System.out.println("\nRunning: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
       System.out.println(String.valueOf(res.getString(1)) + "\t" + String.valueOf(res.getString(2)));
               //+ "\t" + res.getString(2));
    }
    
    


    
    //create table from original table
    sql = " create table if not exists tweets55 as select * from tweets5";
    System.out.println("Running: " + sql);
    stmt.execute(sql);
    System.out.println("Table from original table created \n");
 
    
    

/*
    //regular query
    sql = "select id,user.time_zone from tweets55 where user.name='Matthew Coutts'";
    // sql = "select * from " + tableName;
    System.out.println("\nRunning: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
       System.out.println(String.valueOf(res.getString(1)) + "\t" + String.valueOf(res.getString(2)));
    }
    */

 /*   
//regular query
    sql = "select id,user.name from tweets55 where user.time_zone='Eastern Time (US & Canada)'";
    // sql = "select * from " + tableName;
    System.out.println("\nRunning: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
       System.out.println(String.valueOf(res.getString(1)) + "\t" + String.valueOf(res.getString(2)));
    }
*/


    
    //Dictionary Table
    System.out.println("\n Creating Dictionary Table \n");
    stmt.execute("CREATE EXTERNAL TABLE IF NOT EXISTS"
            + " dictionary (type string,length int,"
            + "word string,pos string,stemmed string,"
            + "polarity string)"
            + " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"
            + "STORED AS TEXTFILE LOCATION '/home/aj/dictionary2'" );
     System.out.println("Dictionary Table created \n");
    



     //Time Zone Map Table
     System.out.println("\nCreating Time Zone Map Table \n");
    stmt.execute("CREATE EXTERNAL TABLE IF NOT EXISTS time_zone_map"
            + " (time_zone string,country string,notes string)"
            + " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"
            + "STORED AS TEXTFILE LOCATION '/home/aj/timezonemap1'");
     System.out.println("Time Zone Map Table created \n");
     

     

//different views
     stmt.execute("CREATE VIEW IF NOT EXISTS tweets_simple AS SELECT"
             + " id,cast( from_unixtime( unix_timestamp"
             + "(concat( '2014 ', substring(created_at,5,15)),"
             + " 'yyyy MMM dd hh:mm:ss')) as timestamp) ts,text,user.time_zone"
             + " FROM tweets55");
     
     System.out.println("\nTweets_simple VIEW created \n");
     
     

//for time_zone
     stmt.execute("CREATE VIEW IF NOT EXISTS tweets_clean"
             + " AS SELECT id, ts, text, m.country FROM"
             + " tweets_simple t LEFT OUTER JOIN"
             + " time_zone_map m ON t.time_zone = m.time_zone");
     System.out.println("\nTweets_clean VIEW created \n");
     
     
      
    sql = "CREATE TABLE IF NOT EXISTS tweets_clean1 as select DISTINCT id,ts,text,country from tweets_clean where id is not NULL AND text is not NULL";
    System.out.println("Running: " + sql);
    stmt.execute(sql);
   
     
     
     //create table to perform map reduce operation from original table
    sql = " create table if not exists tweets555 as select DISTINCT id,text from tweets55 where id is not NULL";
    System.out.println("Running: " + sql);
    stmt.execute(sql);
    System.out.println("Table from original table created \n");
     
     System.out.println("Beginning Tokenisation and Normalisation \n");
     
    //mapreduce code 
    // pn1 pnobj= new pn1();
    // pnobj.PNcall();
     
     
      //create table to perform map reduce operation from original table
    sql = "create external table IF NOT EXISTS tweets_id2 (id string, polarity int) row format delimited fields terminated by '\t' location '/user/hduser/output53'";
    System.out.println("Running: " + sql);
    stmt.execute(sql);
    System.out.println("Loading sentiment analysis data into hive \n");
    
    
     sql = "select id,polarity from tweets_id2";
    // sql = "select * from " + tableName;
    System.out.println("\nRunning: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
       System.out.println(String.valueOf(res.getString(1)) + String.valueOf(res.getString(2)));
               //+ "\t" + res.getString(2));
    }
    
    sql = "drop table tweets_id21";
    System.out.println("Running: " + sql);
    stmt.execute(sql);
    
    
    //create table to perform map reduce operation from original table
    sql = "create table IF NOT EXISTS tweets_id21 as select substr(id,1,18),polarity from tweets_id2";
    System.out.println("Running: " + sql);
    stmt.execute(sql);
    System.out.println("Getting id and polarity \n");
    
    
    sql = "ALTER TABLE tweets_id21 CHANGE `_c0` `id` bigint;";
    System.out.println("Running: " + sql);
    stmt.execute(sql);
    System.out.println(" \n");
      
    // describe tables
    sql = "describe tweets_clean";
    res = stmt.executeQuery(sql);
    while (res.next()) 
    {      
        System.out.println(res.getString(1) + "\t" + res.getString(2));
    }
    
   
    
     //create table having id, polarity and country
    sql = "create table IF NOT EXISTS tweetsbi as select q.id, p.country, q.polarity from tweets_clean1 p JOIN tweets_id21 q ON (p.id = q.id)";
    System.out.println("Running: " + sql);
    stmt.execute(sql);
    System.out.println("Getting id and polarity \n");
    
    
    sql = "create table IF NOT EXISTS tweets_sentiment as select id, case when sum( polarity ) > 0 then 'positive' when sum( polarity ) < 0 then 'negative' else 'neutral' end as polarity from tweetsbi group by id";
    System.out.println("Running: " + sql);
    stmt.execute(sql);
    System.out.println("Getting sentiments \n");
    
    // select country,count(country) from tweetsbi group by country;
    
    //select polarity,count(polarity) from tweets_sentiment group by polarity;
    
    //select country,count(polarity) from tweetsbi where polarity=1 group by country;
    
    //ALTER TABLE tweets_sentiment CHANGE `polarity` `sentiment` string;
    
    //create table IF NOT EXISTS tweetsbi1 as select p.id, p.country, p.polarity,q.sentiment from tweetsbi p JOIN tweets_sentiment q ON (p.id = q.id);
    
     
     
    //  stmt.execute("create external table IF NOT EXISTS"
      //        + " tweets_id1 (id string, polarity int)"
        //      + " row format delimited fields terminated by '\\t'"
          //    + " location '/user/hduser/output52'");
     //System.out.println("\nTweets_ID1 created \n");
     
     
     /*
     stmt.execute("create view IF NOT EXISTS l1 "
             + "as select id, words from tweets55 lateral"
             + " view explode(sentences(lower(text)))"
             + " dummy as words");
     
     */

     
   
     
     /*
    stmt.execute("create view IF NOT EXISTS l2"
            + " as select id, word from l1 lateral"
            + " view explode(words)dummy as word");
    
 
    stmt.execute("create view  IF NOT EXISTS l3"
            + " as select id, l2.word, case d.polarity"
            + " when 'negative' then -1 when 'positive'"
            + " then 1 else 0 end as polarity from l2 "
            + "left outer join dictionary d on l2.word = d.word");
    */
    
     /*
    System.out.println("Tokenisation and Normalisation Done!!!");
    
    
    
    System.out.println("\n Assigning Sentiments!");
    
    
   stmt.execute("create table IF NOT EXISTS tweets_sentiment"
           + " as select id, case when sum( polarity ) > 0"
           + " then 'positive' when sum( polarity ) < 0 then"
           + " 'negative' else 'neutral' end as sentiment"
           + " from l3 group by id");
   System.out.println("\nSentiments Assigned!"); 
     
     
     
   
     
       sql = "select id,sentiment from tweets_sentiment";
    // sql = "select * from " + tableName;
    System.out.println("\nRunning: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
       System.out.println(String.valueOf(res.getString(1)) + "\t" + String.valueOf(res.getString(2)));
    }
     
     
    
    System.out.println("\nAssigning polarity country wise Country");
    stmt.execute("CREATE TABLE IF NOT EXISTS"
            + " tweetsbi AS SELECT"
            + " t.id,t.country, case s.sentiment"
            + " when 'positive' then 1"
            + " when 'neutral' then 0"
            + " when 'negative' then -1"
            + " end as sentiment FROM tweets_clean"
            + " t LEFT OUTER JOIN tweets_sentiment"
            + " s on t.id = s.id");
    System.out.println("\nPloarity Assigned");
    
    
    
    System.out.println("\nId            Country Sentiment");
       sql = "select id,country,sentiment from tweetsbi";
    // sql = "select * from " + tableName;
    System.out.println("\nRunning: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
       System.out.println(String.valueOf(res.getString(1))
               + "\t" + String.valueOf(res.getString(2))
               + "\t" + String.valueOf(res.getString(3)));
    }
     
     */



                   
        System.out.println("Operation done successfully.");

			

			 stmt.close();
			 conn.close();
		
		
		

		


		System.out.println("End");
}catch(SQLException se){
			se.printStackTrace();
		}
                
catch(Exception e){
			e.printStackTrace();
		}
    

}
  
     
  
	public static void main(String[] args) throws SQLException {
            
            Hive h = new Hive();
            h.hive1();
		
                
}



}
   