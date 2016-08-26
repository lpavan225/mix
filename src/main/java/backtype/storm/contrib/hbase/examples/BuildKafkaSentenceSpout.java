package backtype.storm.contrib.hbase.examples;

import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;

public class BuildKafkaSentenceSpout extends  KafkaSpout{



	  public BuildKafkaSentenceSpout(SpoutConfig spoutConf) {
		super(spoutConf);
		// TODO Auto-generated constructor stub
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	List<Values> values;
	  SpoutOutputCollector _collector;
	 

	

	  @Override
	  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    //declarer.declare(new Fields("shortid","site_id", "inv_vol", "inv_tank", "date","refill","dispensed"));
	   // declarer.declare(new Fields("shortid","site_id", "refill" ,"inv_vol", "inv_tank","dispensed","date"));
	    declarer.declare(new Fields("word"));
	  }

	
}

