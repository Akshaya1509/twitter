package twitter;

import java.util.*;
import java.sql.*;
import org.json.JSONObject;


import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class tw_consumer {
	 private ConsumerConnector consumerConnector = null;
     private final String topic = "messages";

     public void initialize() {
           Properties props = new Properties();
           props.put("zookeeper.connect", "localhost:2181");
           props.put("group.id", "testgroup");
           props.put("zookeeper.session.timeout.ms", "400");
           props.put("zookeeper.sync.time.ms", "300");
           props.put("auto.commit.interval.ms", "1000");
           ConsumerConfig conConfig = new ConsumerConfig(props);
           consumerConnector = Consumer.createJavaConsumerConnector(conConfig);
     }//initialize

     public void consume() throws Exception{
           //Key = topic name, Value = No. of threads for topic
           Map<String, Integer> topicCount = new HashMap<String, Integer>();       
           topicCount.put(topic, new Integer(1));
          
           //ConsumerConnector creates the message stream for each topic
           Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams =
                 consumerConnector.createMessageStreams(topicCount);         
          
           // Get Kafka stream for topic 'json_tw'
           List<KafkaStream<byte[], byte[]>> kStreamList =
                                                consumerStreams.get(topic);
           
           String line = "";
           Integer ifor =0,iwhile=0;
           // Iterate stream using ConsumerIterator
           for (final KafkaStream<byte[], byte[]> kStreams : kStreamList) {
               ConsumerIterator<byte[], byte[]> consumerIte = kStreams.iterator();
               ifor = ifor +1;
               System.out.println("in for again..."+ifor);  
               while (consumerIte.hasNext()){
            	   iwhile =  iwhile +1;
            	  System.out.println("in while again..."+iwhile);
                   //System.out.println("Message consumed from topic  [" + topic + "] : "+new String(consumerIte.next().message())); 
                         
                   line += new String (consumerIte.next().message());
                  
                   /*//json_trial
                   JSONObject obj = new JSONObject(line);
                   //db connectivity
                   try{  
       				   Class.forName("com.mysql.jdbc.Driver");  
       				   Connection con=DriverManager.getConnection(  
       				      "jdbc:mysql://localhost:3306/twitter","root","root");  
       				      //here twitter is database name, root is username and password  
       				
       				   String tweet = obj.getString("id");
       				   String query = "insert into gnip_sample (tweetid) values (?)";
       				   PreparedStatement preparedStmt = con.prepareStatement(query);
       				   preparedStmt.setString (1, tweet);
       				   preparedStmt.execute();
       				   con.close();
                       } catch(Exception e)
                       { System.out.println(e);} //try 
                       */
                   System.out.println(line);
   
               }//while
           
               //Shutdown the consumer connector
               if (consumerConnector != null)   consumerConnector.shutdown();          
           }//for
           
     }//consume
     
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		 tw_consumer tw_consumer = new tw_consumer();
         // Configure Kafka consumer
         tw_consumer.initialize();
         // Start consumption
         tw_consumer.consume();

	}

}
