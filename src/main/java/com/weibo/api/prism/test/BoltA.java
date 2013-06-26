package com.weibo.api.prism.test;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;


import com.weibo.api.prism.storm.bolt.PrismTemplateBolt;
import com.weibo.api.prism.storm.core.PrismComponentStream;
import com.weibo.api.prism.storm.core.PrismScribeLog;

//@PrismComponentStream(id="bolt.prism.scribe.test.level1",
//	stream="stream.prism.scribe.test.root",
//	logFilter = TestPrismScribeLogFilterLevel1.class
//		)
public class BoltA extends PrismTemplateBolt {
	private static final long serialVersionUID = -790152506705449078L;
	private static final Logger LOG = Logger.getLogger(BoltA.class);

	@Override
	public List<List<Object>> onMessage(List<Object> values) {
		PrismScribeLog psl = (PrismScribeLog)values.get(0);
		LOG.info("BOLT_A__:test level 1 prism scribe log received:" + psl);
		List<Object> tuples = new ArrayList<Object>();
		tuples.add(psl.getIp());
		tuples.add(psl.getModule());
		List<List<Object>> tuplesList = new ArrayList<List<Object>>();
		tuplesList.add(tuples);
		return tuplesList;
	}

	@Override
	public String[] declaredFields(){
		return new String[]{"fields.prism.scribe.test.level1","fields.prism.scribe.test.level2"};
	}
	
	@Override
	public String[] declaredStreamID(){
		return new String[]{"stream.prism.scribe.test.level1"};
	}
}
