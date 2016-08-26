package backtype.storm.contrib.hbase.examples;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class TestSpout implements IRichSpout {

  List<Values> values;
  SpoutOutputCollector _collector;

  @SuppressWarnings("rawtypes")
  @Override
  public void open(Map conf, TopologyContext context,
      SpoutOutputCollector collector) {
    this._collector = collector;
    values = new ArrayList<Values>();
    String csvFile = "/home/hadoop/combinvdel_209093_correction.csv" ; //"/Users/mkyong/csv/country.csv";
    String line = "";
    String cvsSplitBy = ",";
    
    try  {
    	BufferedReader br = new BufferedReader(new FileReader(csvFile));
        while ((line = br.readLine()) != null) {

            // use comma as separator
            String[] data = line.split(cvsSplitBy);
           // System.out.println("Country [code= " + data[4] + " , name=" + data[5] + "]");
          values.add(new Values(data[0],  data[1], data[2],    data[3],  data[4], data[5],data[6]));
        }					//"shortid","site_id", "inv_vol", "inv_tank", "date","refill","dispensed"
        								//334549,     18688,     1,     05-03-2014, 51106.08912, 38310.88912	
        							//   site_id=1  inv_vol=334549 inv_tank=18688  date=1 refill,  value=05-03-2014 dispensed,  value=51106.08912
        
    } catch (IOException e) {
        e.printStackTrace();
    }

    /*values.add(new Values("http://bit.ly/LsaBa", "www.baltimoreravens.com/", "kinley", "20120816"));
    values.add(new Values("http://bit.ly/2VL7eA", "www.49ers.com/", "kinley", "20120816"));
    values.add(new Values("http://bit.ly/9ZJhuY", "www.buccaneers.com/index.html", "kinley", "20120816"));
    values.add(new Values("http://atmlb.com/7NG4sm", "baltimore.orioles.mlb.com/", "kinley", "20120816"));*/
  }

  @Override
  public void close() {
  }

  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  @Override
  public void nextTuple() {
    int rand = (int) (Math.random() * 1000);
    _collector.emit(values.get(rand % values.size()));
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    //declarer.declare(new Fields("shortid","site_id", "inv_vol", "inv_tank", "date","refill","dispensed"));
   // declarer.declare(new Fields("shortid","site_id", "refill" ,"inv_vol", "inv_tank","dispensed","date"));
    declarer.declare(new Fields("shortid","site_id", "inv_vol" ,"inv_tank", "date","refill","dispensed"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

}
