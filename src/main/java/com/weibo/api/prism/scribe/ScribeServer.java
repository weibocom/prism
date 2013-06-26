package com.weibo.api.prism.scribe;

import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;

import com.weibo.api.prism.scribe.scribe.Iface;
import com.weibo.api.prism.scribe.scribe.Processor;

public class ScribeServer {
	private static final Logger LOG = Logger.getLogger(ScribeServer.class);
	private TServer server;
	private int port;
	private Processor processor;

	
	public ScribeServer(int port, Processor processor) {
		this.port = port;
		this.processor = processor;
	}
	
	private void startServer(){
		try{
			final TNonblockingServerSocket socket = new TNonblockingServerSocket(this.port);
	    	TNonblockingServer.Args targs = new TNonblockingServer.Args(socket);
	    	targs.maxReadBufferBytes = 102400000;
	    	targs.transportFactory(new TFramedTransport.Factory());
	    	targs.processor(processor);
	    	server = new TNonblockingServer(targs);
	    	new Thread(){
				@Override
				public void run() {
					server.serve();
				}
	    		
	    	}.start();
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}
	
	public void stopServer(){
		this.server.stop();
	}

	/**
	 * 5007为server启动的端口；
	 * 启动参数示例  5007
	 * @param args
	 * @throws ClassNotFoundException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public static void main(String args[]) throws InstantiationException, IllegalAccessException, ClassNotFoundException{
		if(!validateArgs(args)){
			LOG.warn("ScribeServer Start Usage:[serverPort handlerClass]");
			System.exit(-1);			
		}
		int scribeServerPort = Integer.parseInt(args[0].trim());
		String handlerStr = args[1];
		Iface handler = (Iface)ClassUtils.getClass(ScribeServer.class.getClassLoader(), handlerStr).newInstance();
		LOG.info("Scribe Server Info[port:" + scribeServerPort + " handler: + " + handlerStr + "]");
		
		Processor processor = new Processor(handler);
		ScribeServer server = new ScribeServer(scribeServerPort, processor);
		server.startServer();
	}
	
	private static boolean validateArgs(String[] args){
		if(args == null || args.length < 2){
			return false;
		}
		// 验证端口必须是一个有效的数字（未验证端口值的范围）
		String serverPort = args[0];
		if(StringUtils.isBlank(serverPort) || !StringUtils.isNumeric(serverPort)){
			return false;
		}
		return true;
	}
}

