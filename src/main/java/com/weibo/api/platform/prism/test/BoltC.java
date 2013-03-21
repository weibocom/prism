package com.weibo.api.platform.prism.test;
import java.util.List;

import org.apache.log4j.Logger;


import com.weibo.api.platform.prism.storm.bolt.PrismTemplateBolt;
import com.weibo.api.platform.prism.storm.core.PrismComponentStream;
import com.weibo.api.platform.prism.storm.core.PrismScribeLog;
//
//@PrismComponentStream(id="bolt.prism.scribe.test",
//	stream="stream.prism.scribe.test",
//	logFilter = TestPrismScribeLogFilter.class
//		)
//public class BoltC extends PrismTemplateBolt {
//	private static final long serialVersionUID = -790152506705449078L;
//	private static final Logger LOG = Logger.getLogger(BoltC.class);
//
//	@Override
//	public List<List<Object>> onMessage(List<Object> values) {
//		PrismScribeLog psl = (PrismScribeLog)values.get(0);
//		LOG.info("BOLT_C__:prism scribe log received:" + psl);
//		return null;
//	}
//}
