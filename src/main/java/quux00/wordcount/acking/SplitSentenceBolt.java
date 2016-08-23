package quux00.wordcount.acking;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitSentenceBolt extends BaseRichBolt {

  private static final long serialVersionUID = 3092938699134129356L;
  
  private OutputCollector collector;
  private Integer count;
  
  
  @Override @SuppressWarnings("rawtypes")
  public void prepare(Map cfg, TopologyContext topologyCtx, OutputCollector outCollector) {
    System.out.println("Inside Prepare: Split Sentence bolt");
	collector = outCollector;
    count = 0; 
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	System.out.println("Inside declareOutputFields bolt");  
    declarer.declare(new Fields("word"));
  }

  @Override
  public void execute(Tuple tuple) {
    Object value = tuple.getValue(0);
    String sentence = null;
    System.out.println("Inside Split Sentence Bolt");
    count = count + 1;
    if ( count > 10 ) 
    {
    	System.out.println("Consumed 10 messages - Reset the count to zero");
    	count = 0;
    }
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

    String[] words = sentence.split("\\s+");
    for (String word : words) {
      collector.emit(tuple, new Values(word));
    }
    collector.ack(tuple);
  }
}
