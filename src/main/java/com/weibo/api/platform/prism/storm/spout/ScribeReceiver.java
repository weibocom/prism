package com.weibo.api.platform.prism.storm.spout;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;

import com.facebook.fb303.fb_status;
import com.weibo.api.platform.prism.scribe.LogEntry;
import com.weibo.api.platform.prism.scribe.ResultCode;
import com.weibo.api.platform.prism.scribe.scribe;
import com.weibo.api.platform.prism.scribe.scribe.Iface;

public class ScribeReceiver {
	private static final Logger LOG = Logger.getLogger(ScribeReceiver.class);
	
    LinkedBlockingQueue<String> _events;
    volatile boolean _active = false;
    volatile boolean _scribeActive = false;
    final Object _scribeLock = new Object();
    TServer _server;

    public static LinkedBlockingQueue<String> makeEventsQueue(Map conf) {
        Number bufferSize = (Number) conf.get("scribe.spout.buffer.size");
        if(bufferSize==null) bufferSize = 50000;
        return new LinkedBlockingQueue<String>(bufferSize.intValue());
    }
    
    public ScribeReceiver(LinkedBlockingQueue<String> events, Map conf, TopologyContext context) {
        _events = events;
        
        Number putTimeout = (Number) conf.get("scribe.spout.put.timeout");
        if(putTimeout==null) putTimeout = 5;
        
        int port = 1651;

        String host;
        try {
            host = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }
        
        ScribeServiceHandler handler = new ScribeServiceHandler(putTimeout.intValue());
        try {
            THsHaServer.Args args = new THsHaServer.Args(new TNonblockingServerSocket(port))
                    .workerThreads(1)
                    .protocolFactory(new TBinaryProtocol.Factory())
                    .processor(new scribe.Processor(handler));
            args.maxReadBufferBytes = 102400000;
            _server = new THsHaServer(args);
            Thread thread = new Thread(new Runnable() {
                    public void run() {
                      _server.serve();
                    }
                  });
            thread.setDaemon(true);
            thread.start();
            LOG.info("Scribe Server Started:\t" + host + ":" + port);
        } catch (TTransportException ex) {
        	LOG.info("Scribe Server Start failed:\t" + host + ":" + port, ex);
            throw new RuntimeException(ex);
        }
    }
    
    public void shutdown() {
        _server.stop();
    }
    
    public void activate() {
        _active = true;
    }

    public void deactivate() {
        _active = false;
        while(true) {
            synchronized(_scribeLock) {
                if(_scribeActive) {
                    Utils.sleep(10);
                } else {
                    break;
                }
            }
        }
    }
    
    private class ScribeServiceHandler implements Iface {
        long _startTime;
        int _putTimeoutSecs;
        
        public ScribeServiceHandler(int putTimeoutSecs) {
            _startTime = System.currentTimeMillis();
            _putTimeoutSecs = putTimeoutSecs;
        }
        
        @Override
        public ResultCode Log(List<LogEntry> messages)  throws TException{
            synchronized(_scribeLock) {
                if(!_active) return ResultCode.TRY_LATER;
                _scribeActive = true;
            }
            for(LogEntry le: messages) {
                try {
                    String o = le.getMessage();
                    boolean taken = _events.offer(o, _putTimeoutSecs, TimeUnit.SECONDS);
                    if(!taken) return ResultCode.TRY_LATER;
                } catch(InterruptedException ex) {
                    throw new TException(ex);
                }
            }
            //TODO: for a reliable spout, need to *block* (with timeout) here until every single tuple gets acked
            _scribeActive = false;
            return ResultCode.OK;
        }

        @Override
        public void shutdown() throws TException {
            
        }

        @Override
        public String getName() throws TException {
            return "rainbird-scribe-spout";
        }

        @Override
        public String getVersion() throws TException {
            return "0.0.1";
        }

        @Override
        public fb_status getStatus() throws TException {
            return fb_status.ALIVE;
        }

        @Override
        public String getStatusDetails() throws TException {
            return "n/a";
        }

        @Override
        public Map<String, Long> getCounters() throws TException {
            return new HashMap<String, Long>();
        }

        @Override
        public long getCounter(String key) throws TException {
            return 0L;
        }

        @Override
        public void setOption(String key, String value) throws TException {
        }

        @Override
        public String getOption(String key) throws TException {
            return "";
        }

        @Override
        public Map<String, String> getOptions() throws TException {
            return new HashMap<String, String>();
        }

        @Override
        public String getCpuProfile(int profileDurationInSec) throws TException {
            return "n/a";
        }

        @Override
        public long aliveSince() throws TException {
            return _startTime;
        }

        @Override
        public void reinitialize() throws TException {
        }

    }
}
