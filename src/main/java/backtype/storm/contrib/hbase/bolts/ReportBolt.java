package backtype.storm.contrib.hbase.bolts;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class ReportBolt extends BaseRichBolt {

	private static final long serialVersionUID = 6102304822420418016L;

	private Map<String, Long> counts;
	private OutputCollector collector;

	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector outCollector) {
		collector = outCollector;
		counts = new HashMap<String, Long>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// terminal bolt = does not emit anything
	}

	@Override
	public void execute(Tuple tuple) {
		String word = tuple.getString(0);
		Long count = tuple.getLong(1);
		System.out.println("Word Reporting Bolt " + word);
		counts.put(word, count);
		collector.ack(tuple);
		BufferedWriter output;
		List<String> keys = new ArrayList<String>();
		try {
			output = new BufferedWriter(new FileWriter("/home/hadoop/hbaseOut.txt", true));
			keys.addAll(counts.keySet());
			Collections.sort(keys);

			for (String key : keys) {
				System.out.println(key + " : " + counts.get(key));
				output.newLine();
				output.append(key);
			}
			output.close();
			System.out.println("--------------");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void cleanup() {
		System.out.println("--- FINAL COUNTS ---");
		List<String> keys = new ArrayList<String>();
		BufferedWriter output;
		try {
			output = new BufferedWriter(new FileWriter("/home/ec2-user/pavan/hh.txt", true));
			keys.addAll(counts.keySet());
			Collections.sort(keys);

			for (String key : keys) {
				System.out.println(key + " : " + counts.get(key));
				output.newLine();
				output.append(key + " : " + counts.get(key));
			}
			output.close();
			System.out.println("--------------");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
