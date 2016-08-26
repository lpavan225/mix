package backtype.storm.contrib.hbase.bolts;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ProcessFuelDispenserBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	
	@Override @SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector outCollector) {
		// TODO Auto-generated method stub
		collector = outCollector;
	}

	@Override
	public void execute(Tuple tuple) {
		Object value = tuple.getValue(0);
	    String sentence = null;
	    
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

	    String[] words = sentence.split(",");
	    System.out.println("words"+words);
	    collector.emit(new Values(words[0],words[1],words[2],words[3],words[4],words[5],words[6]));

	    collector.ack(tuple);
	  }

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("shortid","site_id", "inv_vol" ,"inv_tank", "date","refill","dispensed"));

	}

}

