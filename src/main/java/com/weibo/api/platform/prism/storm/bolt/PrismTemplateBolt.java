package com.weibo.api.platform.prism.storm.bolt;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import com.weibo.api.platform.prism.storm.core.PrismScribeLog;
import com.weibo.api.platform.prism.storm.core.PrismScribeLogColumn;

public abstract class PrismTemplateBolt   extends BaseBasicBolt {
	private static final long serialVersionUID = -3673685354480948622L;
	private static final Logger LOG = Logger.getLogger(PrismTemplateBolt.class);
	
	@Override
    public void prepare(Map stormConf, TopologyContext context) {
		init();
    }
	
	public void init(){
		
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		List<List<Object>> tuples = onMessage(input.getValues());
		if(tuples != null){
			String[] streamIds = declaredStreamID();
			for(List<Object> tuple : tuples){
				for(String streamId : streamIds){
					collector.emit(streamId, tuple);
				}
			}
		}
	}
	
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		String[] fields = declaredFields();
		String[] streamIds = declaredStreamID();
		if(fields != null){
			Fields df = new Fields(fields);
			LOG.info("declared output fields:[" + streamIds + "," + df.toList());
			for(String sid : streamIds){
				declarer.declareStream(sid, df);
			}
		}
	}
	
	protected String[] declaredFields(){
		return null;
	}
	
	protected String[] declaredStreamID(){
		return new String[]{Utils.DEFAULT_STREAM_ID};
	}

	/**
	 * 当有消息到达时，会触发该方法。
	 * @param psl 标准的Prism日志
	 * @return 如果消息处理完成后不需要转发，则返回null，否则返回一个tuple列表。列表中每一个元素为一个tuple(也是一个list)，tuple中元素的定义与declaredFields相同。
	 */
	public abstract List<List<Object>> onMessage(List<Object> values);
}
