package backtype.storm.contrib.hbase.examples;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.contrib.hbase.bolts.HBaseBolt;
import backtype.storm.contrib.hbase.bolts.ProcessBolt;
import backtype.storm.contrib.hbase.bolts.ProcessFuelDispenserBolt;
import backtype.storm.contrib.hbase.bolts.ProcessFuelPriceBolt;
import backtype.storm.contrib.hbase.bolts.ProcessHolidaysBolt;
import backtype.storm.contrib.hbase.bolts.ProcessWeatherDataBolt;
import backtype.storm.contrib.hbase.utils.TupleTableConfig;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 * An example non-transactional topology that uses the {@link HBaseBolt} to
 * insert a stream of shortened URL's into a HBase table called 'shorturl'.
 * <p>
 * Assumes the HBase table has been created.<br>
 * <tt>create 'shorturl', {NAME => 'data', VERSIONS => 3}, {NAME => 'daily', VERSION => 1, TTL => 604800}</tt>
 */
public class DifferentTopicsTopology {
	/**
	 * @param args
	 */
	private static final String FUEL_DISPENSER_SPOUT = "fuel-dispenser-spout";
	private static final String FUEL_PRICES_SPOUT = "fuel-prices-spout";
	private static final String WEATHER_DATA_SPOUT = "weather-data-spout";
	private static final String HOLIDAY_SPOUT = "holiday-spout";

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

		String zkHostPort = "localhost:2181";

		// fuel Dispenser
		String fuelDispenserTopic = "fuelDispenserData";
		String zkRoot = "/fuelDispenser-spout";
		String zkSpoutId = "fuelDispenser-spout";
		ZkHosts zkHosts = new ZkHosts(zkHostPort);
		SpoutConfig fuelDispenserSpoutCfg = new SpoutConfig(zkHosts, fuelDispenserTopic, zkRoot, zkSpoutId);
		KafkaSpout fuelDispenserSpout = new BuildKafkaSentenceSpout(fuelDispenserSpoutCfg);
		
		
		// fuel Dispenser
		String fuelPriceTopic = "fuelPriceData";
		String fuelPricezkRoot = "/fuelDispenser-spout";
		String fuelPricezkSpoutId = "fuelDispenser-spout";
		//ZkHosts zkHosts = new ZkHosts(zkHostPort);
		SpoutConfig fuelPriceSpoutCfg = new SpoutConfig(zkHosts, fuelPriceTopic, fuelPricezkRoot, fuelPricezkSpoutId);
		KafkaSpout fuelPriceSpout = new BuildKafkaSentenceSpout(fuelPriceSpoutCfg);
		
		// Holiday 
		String holidayTopic = "holidayData";
		String holidayzkRoot = "/holiday-spout";
		String holidayzkSpoutId = "holiday-kafka-spout";
		//ZkHosts zkHosts = new ZkHosts(zkHostPort);
		SpoutConfig holidaySpoutCfg = new SpoutConfig(zkHosts, holidayTopic, holidayzkRoot, holidayzkSpoutId);
		KafkaSpout holidaySpout = new BuildKafkaSentenceSpout(holidaySpoutCfg);
		
		// Weather 
		String weatherTopic = "weatherData";
		String weatherzkRoot = "/weather-spout";
		String weatherzkSpoutId = "weather-kafka-spout";
		//ZkHosts zkHosts = new ZkHosts(zkHostPort);
		SpoutConfig weatherSpoutCfg = new SpoutConfig(zkHosts, weatherTopic, weatherzkRoot, weatherzkSpoutId);
		KafkaSpout weatherSpout = new BuildKafkaSentenceSpout(weatherSpoutCfg);
		
		TopologyBuilder builder = new TopologyBuilder();
		
		

		// Build TupleTableConifg
		TupleTableConfig fuelDisenserConfig = new TupleTableConfig("fuel_dispenser_test", "shortid");

		TupleTableConfig holidayConfig = new TupleTableConfig("us_holidays_test", "shortid");

		TupleTableConfig fuelPricesConfig = new TupleTableConfig("fuel_prices_test", "shortid");

		TupleTableConfig weatherConfig = new TupleTableConfig("weather_data_test", "shortid");

		fuelDisenserConfig.setBatch(false);
		holidayConfig.setBatch(false);
		fuelPricesConfig.setBatch(false);
		weatherConfig.setBatch(false);
		// fuel dispenser : site_id,inv_vol,inv_tank_num,date,refill,dispensed
		fuelDisenserConfig.addColumn("CF1", "site_id");
		fuelDisenserConfig.addColumn("CF1", "inv_vol");
		fuelDisenserConfig.addColumn("CF1", "inv_tank");
		fuelDisenserConfig.addColumn("CF1", "date");
		fuelDisenserConfig.addColumn("CF1", "refill");
		fuelDisenserConfig.addColumn("CF1", "dispensed");
		// us holidays : datestr,holiday
		holidayConfig.addColumn("CF1", "datestr");
		holidayConfig.addColumn("CF1", "holiday");
		// fule prices : Datestr,Fuel_Price,State
		fuelPricesConfig.addColumn("CF1", "Datestr");
		fuelPricesConfig.addColumn("CF1", "Fuel_Price");
		fuelPricesConfig.addColumn("CF1", "State");
		// weather data : Date,Precip,Max_Air_Temp,Min_Air_Temp,Avg_Air_Temp
		weatherConfig.addColumn("CF1", "Date");
		weatherConfig.addColumn("CF1", "Precip");
		weatherConfig.addColumn("CF1", "Max_Air_Temp");
		weatherConfig.addColumn("CF1", "Min_Air_Temp");
		weatherConfig.addColumn("CF1", "Avg_Air_Temp");
	

		// Add HBaseBolt
		builder.setSpout(FUEL_DISPENSER_SPOUT, fuelDispenserSpout, 1);
		
		// Fuel Dispenser
		builder.setBolt("processFuelDispenser", new ProcessFuelDispenserBolt()).shuffleGrouping(FUEL_DISPENSER_SPOUT);
		//builder.setBolt("processFuelDispenser", new ProcessFuelDispenserBolt()).fieldsGrouping(SENTENCE_SPOUT_ID, new Fields("word"));
		builder.setBolt("fuelDispenserData", new HBaseBolt(fuelDisenserConfig), 1).fieldsGrouping("processFuelDispenser",
				new Fields("shortid", "site_id", "inv_vol", "inv_tank", "date", "refill", "dispensed"));

		// Holidays
		builder.setSpout(HOLIDAY_SPOUT, holidaySpout, 1);
		builder.setBolt("processHoliday", new ProcessHolidaysBolt()).shuffleGrouping(HOLIDAY_SPOUT);
		builder.setBolt("holidayData", new HBaseBolt(holidayConfig), 1).fieldsGrouping("processHoliday",new Fields("shortid", "datestr", "holiday"));
				
		// Weather Data
		builder.setSpout(WEATHER_DATA_SPOUT, weatherSpout, 1);
		builder.setBolt("processWeatherData", new ProcessWeatherDataBolt()).shuffleGrouping(WEATHER_DATA_SPOUT);
		builder.setBolt("weatherData", new HBaseBolt(weatherConfig), 1).fieldsGrouping("processWeatherData",new Fields("shortid", "Date", "Precip","Max_Air_Temp","Min_Air_Temp", "Avg_Air_Temp" ));
		
		
		// fuel Data
		builder.setSpout(FUEL_PRICES_SPOUT, fuelPriceSpout, 1);
		builder.setBolt("processFuelPrice", new ProcessFuelPriceBolt()).shuffleGrouping(FUEL_PRICES_SPOUT);
		builder.setBolt("fuelPrice", new HBaseBolt(holidayConfig), 1).fieldsGrouping("processFuelPrice",new Fields("shortid","Datestr","Fuel_Price","State"));
						
		Config cfg = new Config();
		StormSubmitter.submitTopology("differentTopics", cfg, builder.createTopology());

	}

}

