package com.weibo.api.prism.scribe.handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.facebook.fb303.fb_status;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import com.weibo.api.prism.scribe.LogEntry;
import com.weibo.api.prism.scribe.ResultCode;
import com.weibo.api.prism.scribe.ScribeServer;
import com.weibo.api.prism.scribe.scribe.Iface;

/**
 * 将日志转发到
 * @author crystal
 *
 */
public class ScribeLogForwarderHandler extends com.facebook.fb303.FacebookBase implements Iface, Watcher{
	private static final Logger LOG = Logger.getLogger(ScribeServer.class);
	CuratorFramework  zk;
	String zkStr;
	List<ScribeAsyncClient> scribeClients;
	String regServerPath;
	
	protected ScribeLogForwarderHandler() {
		super("prism.stream.forwarder");
		zkStr = System.getProperty("prism.zk", "localhost:2181");
		regServerPath = System.getProperty("prism.servers.scribe.register", "/prism/servers/scribe/register");
		 try {
			zk = CuratorFrameworkFactory.builder().connectString("localhost:2181").connectionTimeoutMs(5000).retryPolicy(new RetryNTimes(10,3000)).sessionTimeoutMs(3000).build();
		} catch (IOException e) {
			LOG.error("connect to zk(" + zkStr + ") failed:" + e.getMessage(), e);
			throw new RuntimeException("connect to zk(" + zkStr + ") failed:" + e.getMessage(), e);
		}
		zk.start();
		LOG.info("prism.zk started:\t" + zkStr + " the listen register servers is in:" + regServerPath);
		
		refreshScribeClient();
	}

	private void refreshScribeClient() {
		if(scribeClients == null){
			scribeClients = new ArrayList<ScribeAsyncClient>();
		}
		
		try {
			List<String> ipPorts = zk.getChildren().usingWatcher(this).forPath(regServerPath);
			
			for(String ipPort : ipPorts){
				int idx;
				if((idx = ipPort.indexOf(':')) > 0){
					String ip = ipPort.substring(0, idx);
					String portStr = ipPort.substring(idx+1);
					if(StringUtils.isNumeric(portStr)){
						int port = Integer.parseInt(portStr);
						if(!contains(scribeClients, ip, port)){
							ScribeAsyncClient sc = new ScribeAsyncClient(ip, port);
							sc.init();
							scribeClients.add(sc);
						}	
					}
				}
			}
		} catch (Exception e) {
			LOG.error("failed to get children for:" + regServerPath, e);
		}
	}

	private boolean contains(List<ScribeAsyncClient> scs, String ip, int port) {
		for(ScribeAsyncClient sc : scs){
			if(sc.contains(ip, port)){
				return true;
			}
		}
		return false;
	}

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
	
	int read = 0;

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

	@Override
	public void process(WatchedEvent event) {
		zk.
		
	}
	
}