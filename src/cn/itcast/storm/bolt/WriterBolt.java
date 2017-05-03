package cn.itcast.storm.bolt;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
/**
 * 将数据写入文件
 *
 */
public class WriterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -6586283337287975719L;
	
	private BufferedWriter bufferedWriter = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		try {
			bufferedWriter = new BufferedWriter(new FileWriter("/home/kang/kafka-storm-test/"+UUID.randomUUID().toString()));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
	
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String s = input.getString(0);
		try {
			bufferedWriter.write(s);
			bufferedWriter.newLine();
			bufferedWriter.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}

