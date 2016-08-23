package posfeed.inventory.acking;

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
import backtype.storm.tuple.Values;

public class InventoryFileBolt extends BaseRichBolt {

  private static final long serialVersionUID = 4092938699134129356L;
  
  private OutputCollector collector;
  private Integer msgCount;
  private DateFormat df;
  private String filedate;
  private File invfile;
  private FileWriter invfilewrite;
  private BufferedWriter invbufwrite;
  private long starttime;
  private long endtime;
  
  
  
  @Override @SuppressWarnings("rawtypes")
  public void prepare(Map cfg, TopologyContext topologyCtx, OutputCollector outCollector) {
    System.out.println("Inside Prepare: Inventory File Bolt");
	collector = outCollector;
    msgCount = 0; 
    df = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss:SSS");
    df.setTimeZone(TimeZone.getTimeZone("IST"));
    filedate = df.format(new Date());
    invfile = new File("/tmp/pos_feed/POS_DM_"+filedate+".csv");
    starttime = System.currentTimeMillis();
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
    	        if ( msgCount > 16000 ) 
    	        {
    	        	
    	        	invbufwrite.close();
    	        	endtime = System.currentTimeMillis() - starttime;
    	        	System.out.println("File with "+ msgCount + "messages written in "+ endtime +"ms");
    	        	msgCount = 0;
    	        	 df = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss:SSS");
    	        	    df.setTimeZone(TimeZone.getTimeZone("IST"));
    	        	    filedate = df.format(new Date());
    	        	    invfile = new File("/tmp/pos_feed/POS_DM_"+filedate+".csv");
    	        	    if (!invfile.exists())
            	    	{
            	    		invfile.createNewFile();
            	    	}
            			invfilewrite = new FileWriter(invfile.getAbsoluteFile());
            			invbufwrite = new BufferedWriter(invfilewrite); 
            			starttime = System.currentTimeMillis();
    	        }		
    	} catch (IOException e1) {
    		// TODO Auto-generated catch block
    		e1.printStackTrace();
        }
       
        collector.ack(tuple);
        }



}  // Main class