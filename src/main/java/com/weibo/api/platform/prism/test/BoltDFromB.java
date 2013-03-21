package com.weibo.api.platform.prism.test;

import java.util.List;

import org.apache.log4j.Logger;

import com.weibo.api.platform.prism.storm.bolt.PrismTemplateBolt;
import com.weibo.api.platform.prism.storm.core.PrismComponentStream;

//@PrismComponentStream(id="bolt.prism.scribe.test.d",
//stream="default",
//source = "bolt.prism.scribe.test.level2"
//	)
public class BoltDFromB extends PrismTemplateBolt {
	private static final Logger LOG = Logger.getLogger(BoltDFromB.class);
	@Override
	public List<List<Object>> onMessage(List<Object> values) {
		LOG.info(values);
		return null;
	}

}
