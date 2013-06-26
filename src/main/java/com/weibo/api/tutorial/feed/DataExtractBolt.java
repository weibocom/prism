package com.weibo.api.tutorial.feed;

import java.util.List;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.weibo.api.prism.storm.Constants;
import com.weibo.api.prism.storm.bolt.PrismTemplateBolt;
import com.weibo.api.prism.storm.core.PrismComponentStream;
import com.weibo.api.prism.storm.core.PrismComponentStream.StreamGroupingPolocy;
import com.weibo.api.prism.storm.core.PrismScribeLog;

@PrismComponentStream(
		id="bolt.feedmonitor.visitstatus.total",
		source=Constants.SPOUT_ID_PRISM_ROOT,
		stream="bolt.feedmonitor.visitstatus.total",
		groupingPolocy=StreamGroupingPolocy.SHUFFLE,
		logFilter=FeedMonitorPrismScribeLogFilter.class,
		parallelism=1
		)
public class DataExtractBolt extends PrismTemplateBolt {
	private static final Logger LOG = Logger.getLogger(DataExtractBolt.class);
	
	int total = 0;
	long lastFlushTime = 0;
	int intervalSeconds = 3;//每隔三秒钟输出一次统计数据
	
	public void init(){
		lastFlushTime = System.currentTimeMillis();		
	}

	@Override
	public List<List<Object>> onMessage(List<Object> values) {
		if(System.currentTimeMillis() - lastFlushTime >= intervalSeconds){
			LOG.info("the total feed average request is:" + (total * 1.0 / intervalSeconds));
			total = 0;
			lastFlushTime=System.currentTimeMillis();
		}
		try{
			PrismScribeLog psl = (PrismScribeLog) values.get(0);
			JSONParser parser = new JSONParser();
			JSONObject json = (JSONObject) parser.parse((String) psl.getData());
			int t = ((Number)json.get("total")).intValue();
			total+=t;
			return null;
		} catch(Exception e){
			LOG.error("feed monitor visit status data parse error:" + e.getMessage(), e);
			return null;
		}
	}

}
