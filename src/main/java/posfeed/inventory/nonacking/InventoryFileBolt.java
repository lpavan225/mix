package posfeed.inventory.nonacking;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.TimeZone;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;


public class InventoryFileBolt extends BaseRichBolt {

  private static final long serialVersionUID = 4092938699134129356L;
  
  private Integer msgCount;
  private DateFormat df;
  private String filedate;
  private File invfile;
  private FileWriter invfilewrite;
  private BufferedWriter invbufwrite;
  
  
  @Override @SuppressWarnings("rawtypes")
  public void prepare(Map cfg, TopologyContext topologyCtx, OutputCollector outCollector) {
    System.out.println("Inside Prepare: Inventory File Bolt");
	
    msgCount = 0; 
    df = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss:SSS");
    df.setTimeZone(TimeZone.getTimeZone("IST"));
    filedate = df.format(new Date());
    invfile = new File("/tmp/pos_feed/POS_DM_"+filedate+".csv");
    try {
    	if (!invfile.exists())
    	{
    		invfile.createNewFile();
    	}
		invfilewrite = new FileWriter(invfile.getAbsoluteFile());
		invbufwrite = new BufferedWriter(invfilewrite); 
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	// terminal bolt - does not emit anything
  }

  @Override
  public void execute(Tuple tuple) {
       	Object value = tuple.getValue(0);
        String sentence = null;
        System.out.println("Inside Split Sentence Bolt");
           
        if (value instanceof String) {
          sentence = (String) value;

        } else {
          // Kafka returns bytes
          byte[] bytes = (byte[]) value;
          try {
            sentence = new String(bytes, "UTF-8");
          } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
          }      
        }
        try {
    		invbufwrite.write(sentence);
    		invbufwrite.newLine();
    		  msgCount = msgCount + 1;
    	        if ( msgCount > 10 ) 
    	        {
    	        	System.out.println("Consumed 10 messages - Reset the count to zero");
    	        	msgCount = 0;
    	        	 df = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
    	        	    df.setTimeZone(TimeZone.getTimeZone("IST"));
    	        	    filedate = df.format(new Date());
    	        	    invfile = new File("/tmp/pos_feed/POS_DM_"+filedate+".csv");
    	        	    if (!invfile.exists())
            	    	{
            	    		invfile.createNewFile();
            	    	}
            			invfilewrite = new FileWriter(invfile.getAbsoluteFile());
            			invbufwrite = new BufferedWriter(invfilewrite); 
    	        }		
    	} catch (IOException e1) {
    		// TODO Auto-generated catch block
    		e1.printStackTrace();
        }
       
       
        }



}  // Main class