/**
 * 
 */
package com.weibo.api.prism.scribe.handler;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;

import com.weibo.api.prism.scribe.LogEntry;
import com.weibo.api.prism.scribe.scribe.Client;

/**
 * @author jolestar
 * 
 */
public class ScribeAsyncClient implements Serializable{
	private static final Logger LOG = Logger.getLogger(DefaultScribeLogHandler.class);

	private Queue<LogEntry> logQueue = new ConcurrentLinkedQueue<LogEntry>();

	private AtomicBoolean remoteAvailable = new AtomicBoolean(false);

	private ScheduledExecutorService scheduledExector = Executors
			.newScheduledThreadPool(1);

	private AsyncLog asyncLog = null;

	private Appender failLoggerAppender;
	

	static Logger scribeError = Logger.getLogger("scribe_error");
	
	
	private Map<String,Logger> failLoggers;


	/**
	 * thrift so timeout
	 */
	private static final int DEFAULT_SO_TIMEOUT = 1000;

	private int timeout = DEFAULT_SO_TIMEOUT;

	// Thrift-specific
	private volatile Client client;
	private TSocket sock;
	private TFramedTransport transport;
	private TBinaryProtocol protocol;

	private String hostname;
	private int port;

	public ScribeAsyncClient(String hostname, int port) {
		this.hostname = hostname;
		this.port = port;
	}
	
	public boolean contains(String host, int port){
		return hostname.equals(host) && this.port == port;
	}

	public void init() {
		this.connect();
		asyncLog = new AsyncLog();
		asyncLog.start();
		//500毫秒检查一次
		scheduledExector.scheduleWithFixedDelay(new Runnable() {
			
			@Override
			public void run() {
				connect();
			}
		},500, 1000,TimeUnit.MILLISECONDS);
		failLoggers = new ConcurrentHashMap<String, Logger>();
	}
	
	private synchronized void connect() {

		if (remoteAvailable.get()) {
			return;
		}
		// 如果是thrift写入出错，则关闭后重新初始化。
		if (this.sock != null && this.sock.isOpen()) {
			this.sock.close();
		}
		try {
			this.sock = new TSocket(hostname, port, timeout);
			this.sock.open();
			this.transport = new TFramedTransport(sock);
			this.protocol = new TBinaryProtocol(transport, false, false);
			this.client = new Client(protocol, protocol);
			remoteAvailable.set(true);
		} catch (Exception e) {
			// 不使用ApiLogger.error 避免死循环
			scribeError.error(String.format("ScribeClient error %s:%s msg:%s "
					,this.hostname,this.port,e.getMessage()));
		}
	}
		
	private Logger getFailLogger(String category){
		return this.failLoggers.get(category);
	}

	public void append(LogEntry entry) {
		logQueue.add(entry);
	}

	/**
	 * write the log async through a concurrent queue
	 * 
	 * @author tim
	 * @author yuanming
	 */
	class AsyncLog extends Thread {
		public static final int LOOP_COUNT = 100;

		private AtomicBoolean stopLog = new AtomicBoolean(false);
		// 本次读取操作是否有错误
		private AtomicBoolean hasError = new AtomicBoolean(false);
		private List<LogEntry> logEntries = new ArrayList<LogEntry>(LOOP_COUNT);

		public AsyncLog() {
		}

		public void stopLog(boolean stop) {
			stopLog.set(stop);
		}

		@Override
		public void run() {
			LOG.info("Start Scribe thread.");
			do {
				readQueue(LOOP_COUNT, true);
			} while (stopLog.get() == false);
			this.flush();
			LOG.info("Stop Scribe thread.");
		}

		// 将所有内存中的日志都刷新到磁盘
		private void flush() {
			LOG.info("flush Scribe log.");
			int size = 0;
			while ((size = this.readQueue(LOOP_COUNT, false)) > 0) {
				LOG.info("flush Scribe log "
						+ size + " record.");
			}
			LOG.info("flush Scribe end.");
		}

		private final int readQueue(int loopCount, boolean sleepForLessData) {
			int count = 0;
			try {
				LogEntry entry = null;
				while ((entry = logQueue.poll()) != null) {
					logEntries.add(entry);
					if ((count++) >= loopCount)
						break;
				}
				// 如果远程操作不可用，跳转 finally中处理
				if (!remoteAvailable.get()) {
					hasError.set(true);
					return logEntries.size();
				}
				// 如果积累的日志太多，说明thrift接口太慢，暂时写本地
				if (logQueue.size() > 50000) {
					hasError.set(true);
					return logEntries.size();
				}
				if (count > 0) {
					client.Log(logEntries);
					if (scribeError.isDebugEnabled()) {
						scribeError.debug(" write "
								+ logEntries.size() + " to scribe.");
					}
				}
				return logEntries.size();
			} catch (TException ex) {
				if (ex.getMessage() != null) {
					scribeError.error("ScribeAppender thrift error: "
							+ ex.getMessage());
				}
				// Thrift发生异常，如服务器端关闭，是无法自恢复的，需要重新建立连接
				remoteAvailable.set(false);
				hasError.set(true);
				return logEntries.size();
			} catch (Exception e) {
				scribeError.error(
						"ScribeAppender unknow error: " + e.getMessage(), e);
				return 0;
			} finally {
				// 如果写scribe日志的时候发生异常，则记录到本地的failLogger中
				if (hasError.get() && logEntries.size() > 0) {
					scribeError.warn("write " + logEntries.size()
							+ " to faillog.");
					for (LogEntry e : logEntries) {
						String category = e.getCategory();
						Logger failLogger = ScribeAsyncClient.this.getFailLogger(category);
						if(failLogger != null){
							failLogger.info(e.message);
						}else{
							scribeError.warn("can not find failLogger by name: " + category);
						}
					}
				}
				hasError.set(false);
				logEntries.clear();
				//如果queue中的数据太少则sleep一段时间
				if(count < loopCount &&sleepForLessData){
					try{
						Thread.sleep(100);
					}catch(InterruptedException e){}
				}
			}
		}
	}

	public void close() {
		this.scheduledExector.shutdown();
		if (asyncLog != null) {
			asyncLog.stopLog(true);
			try {
				asyncLog.join();
			} catch (InterruptedException e) {
			}
		}
		if(this.failLoggerAppender != null){
			this.failLoggerAppender.close();
		}
		if (transport != null && transport.isOpen()) {
			transport.close();
		}
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((hostname == null) ? 0 : hostname.hashCode());
		result = prime * result + port;
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ScribeAsyncClient other = (ScribeAsyncClient) obj;
		if (hostname == null) {
			if (other.hostname != null)
				return false;
		} else if (!hostname.equals(other.hostname))
			return false;
		if (port != other.port)
			return false;
		return true;
	}
	

}
