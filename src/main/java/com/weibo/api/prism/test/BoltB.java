package com.weibo.api.prism.test;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.weibo.api.prism.storm.bolt.PrismTemplateBolt;
import com.weibo.api.prism.storm.core.PrismComponentStream;

//@PrismComponentStream(id="bolt.prism.scribe.test.level2",
//	stream="stream.prism.scribe.test.level1",
//	source = "bolt.prism.scribe.test.level1"
//		)
public class BoltB extends PrismTemplateBolt {
	private static final long serialVersionUID = -790152506705449078L;
	private static final Logger LOG = Logger.getLogger(BoltB.class);

	@Override
	public List<List<Object>> onMessage(List<Object> values) {
		LOG.info("BOLT_B__test level 2 prism scribe log received:" + values);
		
		List<List<Object>> tupleList = new ArrayList<List<Object>>();
		for(Object o : values){
			List<Object> tuple = new ArrayList<Object>();
			tuple.add(o + "\t" + "太NX了");
			tupleList.add(tuple);
		}
		return tupleList;
	}
	
	@Override
	public String[] declaredFields(){
		return new String[]{"fields.prism.scribe.test.b1"};
	}
}
