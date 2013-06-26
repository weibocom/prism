package com.weibo.api.prism.scribe.handler;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.facebook.fb303.fb_status;
import com.weibo.api.prism.scribe.LogEntry;
import com.weibo.api.prism.scribe.ResultCode;
import com.weibo.api.prism.scribe.scribe.Iface;

public class DefaultScribeLogHandler extends com.facebook.fb303.FacebookBase implements Iface{
	private ScribeAsyncClient[] sacs;
	
	private ZooKeeper zk;
	
	protected DefaultScribeLogHandler() {
		super("prism.stream");
	}
	
	protected void initClient(){
	}

	private static final Logger LOG = Logger.getLogger(DefaultScribeLogHandler.class);
	
	@Override
	public String getCpuProfile(int arg0) throws TException {
		return null;
	}

	@Override
	public String getVersion() throws TException {
		return null;
	}

	@Override
	public fb_status getStatus() {
		return null;
	}
	
	
	
	@Override
	public ResultCode Log(List<LogEntry> messages) throws TException {
		if(messages == null || messages.size() == 0){
			return ResultCode.OK;
		}
		for(LogEntry e : messages){
			LOG.info(e.getMessage());
		}
		return ResultCode.OK;
	}
	
}