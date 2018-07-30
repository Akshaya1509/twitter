package twitter;
import java.io.BufferedReader;
//import java.io.InputStreamReader;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import java.io.*;


public class tw_producer {
	private static Producer<Integer, String> producer;
    private static final String topic= "messages";

    public void initialize() {
          Properties producerProps = new Properties();
          producerProps.put("metadata.broker.list", "localhost:9092");
          producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
          producerProps.put("request.required.acks", "1");
          ProducerConfig producerConfig = new ProducerConfig(producerProps);
          producer = new Producer<Integer, String>(producerConfig);
    }
    public void publishMesssage() throws Exception{            
          //BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));  
           final String FILENAME = "/home/akshaya/eclipse/tweets_18_testing";
          BufferedReader br = null;
  		  FileReader fr = null;

  		try {

  			fr = new FileReader(FILENAME);
  			br = new BufferedReader(fr);

  			String sCurrentLine;

  			br = new BufferedReader(new FileReader(FILENAME));

  			while ((sCurrentLine = br.readLine()) != null) {
  				KeyedMessage<Integer, String> keyedMsg = new KeyedMessage<Integer, String>(topic,sCurrentLine);
  	        producer.send(keyedMsg); // This publishes message on given topic
  			}

  		} catch (IOException e) {

  			e.printStackTrace();

  		} finally {

  			try {

  				if (br != null)
  					br.close();

  				if (fr != null)
  					fr.close();

  			} catch (IOException ex) {

  				ex.printStackTrace();

  			}

  		}

        //Define topic name and message
             
      
      return;
    }
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		tw_producer tw_producer = new tw_producer();
        // Initialize producer
        tw_producer.initialize();            
        // Publish message
        tw_producer.publishMesssage();
        //Close the producer
        producer.close();

	}

}
