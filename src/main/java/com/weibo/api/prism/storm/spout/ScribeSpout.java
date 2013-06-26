package com.weibo.api.prism.storm.spout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.weibo.api.prism.storm.core.PrismComponentStream;
import com.weibo.api.prism.storm.core.PrismScheme;
import com.weibo.api.prism.storm.core.PrismScribeLog;
import com.weibo.api.prism.storm.core.PrismScribeLogColumn;
import com.weibo.api.prism.storm.core.PrismScribeLogFilter;
import com.weibo.api.prism.storm.core.ScribeLogScheme;
import com.weibo.api.prism.storm.scanner.ScannerFactory;

public class ScribeSpout extends BaseRichSpout {
	private static final long serialVersionUID = 7033824708586002378L;
	private static final Logger LOG = Logger.getLogger(ScribeSpout.class);
	
	SpoutOutputCollector _collector;
    LinkedBlockingQueue<String> _events;
    PrismScheme _scheme;
    ScribeReceiver _receiver;
    String _spoutId;
    
    static Map<String, PrismScribeLogFilter> _streams; // key为streamId，value为filter
    
    public ScribeSpout(PrismScheme scheme, String spoutId) {
        _scheme = scheme;
        _spoutId = spoutId;
    }

    public ScribeSpout(String spoutId) {
        this(new ScribeLogScheme(), spoutId);
    }    
    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _events = ScribeReceiver.makeEventsQueue(conf);
        _receiver = new ScribeReceiver(_events, conf, context);
        
        initAndGetStreams();
    }

	/**
	 * @param context
	 * @return 
	 */
	private Map<String, PrismScribeLogFilter> initAndGetStreams() {
		if(_streams == null){
			synchronized (ScribeSpout.class) {
				if(_streams == null){
					_streams = new HashMap<String, PrismScribeLogFilter>();
			        List<Class> clazzes = ScannerFactory.getScanner().children(_spoutId);
			        LOG.info("component scanned:" + clazzes);
			        for(Class c : clazzes){
			        	PrismComponentStream pcs = (PrismComponentStream)c.getAnnotation(PrismComponentStream.class);
			        	if(pcs != null){
			        		String streamId = pcs.stream();
			        		Class fc = pcs.logFilter();
			        		if(_streams.containsKey(streamId)){
			        			LOG.warn("stream[" + streamId + "] already subscribe from the spout.");
			        		}
			        		try {
								_streams.put(streamId, (PrismScribeLogFilter)fc.newInstance());
							} catch (Exception e) {
								LOG.error("new an instance of class " + fc + " failed:" + e.getMessage(), e);
								throw new RuntimeException(e.getMessage(), e);
							}
			        	}
			        }
			        LOG.info("stream loaded:" + _streams);
				}
			}
		}
		return _streams;
	}

	int read = 0;
	long time = System.currentTimeMillis();
    @Override
    public void nextTuple() {
        String o = _events.poll();
        LOG.debug("tuple in spout:" + o);
        if(StringUtils.isEmpty(o)){
        	Utils.sleep(5);
        	return;
        }
		PrismScribeLog psl = _scheme.deserialize(o);
		LOG.debug(o);
		if(read++ % 3000 == 0){
			LOG.info(read + " message has read, start with: " + time + " rate: " + (read * 1.0 * 1000 / (System.currentTimeMillis() - time)) + " "+ o);
		}
		if (psl != null) {
			List<Object> tuples = new ArrayList<Object>();
			tuples.add(psl);
			Map<String, PrismScribeLogFilter> streams = initAndGetStreams();
			for (String sid : streams.keySet()) {
				PrismScribeLogFilter pslf = streams.get(sid);
				if (pslf.accept(psl)) {
					_collector.emit(sid, tuples);
				}
			}
		}
    }

    @Override
    public void activate() {
        _receiver.activate();
    }

    @Override
    public void deactivate() {
        _receiver.deactivate();
        
        // flush buffer on deactivation
        // this might not respect topology-max-spout-pending, but it's the best we can do
        while(!_events.isEmpty()) {
            nextTuple();
        }
    }

    @Override
    public void close() {
        _receiver.shutdown();
    }    
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields fields = new Fields(PrismScribeLogColumn.BYTES.value());
		Map<String, PrismScribeLogFilter> streams = initAndGetStreams();
		for(String sid : streams.keySet()){
			declarer.declareStream(sid, fields);
		}
    }
    
}
