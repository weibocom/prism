package com.weibo.api.platform.prism.storm.bolt;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import com.weibo.api.platform.prism.storm.core.PrismComponentStream;
import com.weibo.api.platform.prism.storm.core.PrismScribeLog;

/**
 * 
 * @author crystal
 *
 */
@PrismComponentStream(id="bolt.prism.stream.socket",
			      stream="stream.prism.stream.socket"
		)
public class PrismStreamSocketBolt extends PrismTemplateBolt implements Runnable{
	private static final Logger LOG = Logger.getLogger(PrismStreamSocketBolt.class);
	
	private static Set<SocketClient> socketClients;
	
	private Thread socketServerThread;
	
	CuratorFramework _zk;
	String _zkRoot;
	String _zkStr;
	
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    	_zkStr = (String)stormConf.get("prism.zk");
    	_zkRoot = (String)stormConf.get("prism.socket.server.path");
    	socketServerThread = new Thread(this);
    	socketServerThread.setDaemon(true);
    	socketServerThread.start();
    	socketClients = new HashSet<PrismStreamSocketBolt.SocketClient>();
    }

	@Override
	public List<List<Object>> onMessage(List<Object> values) {
		if(values == null || values.isEmpty()){
			return null;
		}
		PrismScribeLog psl = (PrismScribeLog)values.get(0);
		for(SocketClient sc : socketClients){
			sc.write(psl);
		}
		return null;
	}
	
	@Override
	public void cleanup(){
		socketServerThread.interrupt();
		for(SocketClient sc : socketClients){
			sc.stop();
		}
		if(_zk != null){
			_zk.close();
		}
		super.cleanup();
	}

	@Override
	public void run() {
		try{
			ServerSocketChannel ssc = ServerSocketChannel.open();
			ssc.configureBlocking(false);
			Selector s = Selector.open();
			int port = 1650;
			registerOnZk(port);
			ssc.socket().bind(new InetSocketAddress(port));
			LOG.info("prism stream socket server stared, listen port:" + port);
			ssc.register(s, SelectionKey.OP_ACCEPT);
			while(true){
				try{
					int n = s.select();
					if(n == 0){
						continue;
					}
					Iterator it = s.selectedKeys().iterator(); 
					while(it.hasNext()){
						SelectionKey key = (SelectionKey) it.next();
						if (key.isAcceptable()) {  
			                ServerSocketChannel server = (ServerSocketChannel) key  
			                        .channel();  
			                SocketChannel channel = server.accept();  
			                registerChannel(s, channel, SelectionKey.OP_READ);  
			                sayHello(channel);  
			            }  
						if (key.isReadable()) {  
			                handle(key);  
			            }
						it.remove();
					}
				} catch(Exception ioe){
					LOG.error("prism stream socket server process error:" + ioe.getMessage(), ioe);
				}
			}
		} catch(IOException ioe){
			LOG.error("prism stream socket server start error:" + ioe.getMessage(), ioe);
		}
	}

	private void registerOnZk(int port)  {
		try{
        String host = InetAddress.getLocalHost().getCanonicalHostName();
        _zk =  CuratorFrameworkFactory.newClient(
                _zkStr,
                6000,
                3000,
                new RetryNTimes(4, 1000));
        _zk.start();
        _zk.create()
           .creatingParentsIfNeeded()
           .withMode(CreateMode.EPHEMERAL)
           .forPath(_zkRoot + "/" + host + ":" + port);
		} catch(Exception e){
			LOG.error("register socket server status on zk error:" + e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}

	private void handle(SelectionKey key) {
		SocketChannel socketChannel = (SocketChannel) key.channel();
		try{
			ByteBuffer buffer = ByteBuffer.allocate(1024);
			buffer.clear(); // Empty buffer
			// Loop while data is available; channel is nonblocking
			int count = socketChannel.read(buffer);
				buffer.flip(); // Make buffer readable
			if (count < 0) {
				// Close channel on EOF, invalidates the key
				socketChannel.close();
				return;
			}
			byte[] bytes = new byte[count];
			buffer.get(bytes);
			int l = bytes.length;
			if(bytes[bytes.length - 1] == '\n' || bytes[bytes.length - 1] == '\r'){
				l--;
				if(bytes[bytes.length - 2] == '\r'){
					l--;
				}
			}
			String cmd = new String(bytes, 0, l);
			String[] splits = cmd.split(" ");
			if(splits == null || splits.length != 3){
		        writeToChannel("ip port pattern", socketChannel);  
		        socketChannel.close();
		        return;
			}
			SocketClient sc = new SocketClient(splits[0], Integer.parseInt(splits[1]),splits[2]);
			synchronized (socketClients) {
				if(socketClients.contains(sc)){
					LOG.info(sc + " has already established, the available clients:" + socketClients);
					writeToChannel("an connection has established before for:" + sc,
							socketChannel);
				} else {
					if (socketClients.size() >= 12) {
						writeToChannel(
								"the connected client is over the max:" + 12,
								socketChannel);
						socketChannel.close();
						return;
					}
					sc.start();
					this.socketClients.add(sc);
					writeToChannel("an connection established for:" + sc,
							socketChannel);
					LOG.info(sc + " connection has established, the available clients:" + socketClients);
				}
			}
		}catch(Exception e){
			LOG.error("handle key error, cannot read data from channel:" + e.getMessage(), e);
			try {
				writeToChannel("can not establish a connection for given ip&port, please listen the port and try again.", socketChannel);
				socketChannel.close();
			} catch (IOException e1) {
			}
		}
		
	}
	
	private static void writeToChannel(String msg, SocketChannel socketChannel) throws IOException{
		ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.clear();  
        buffer.put((msg + "\r\n").getBytes());  
        buffer.flip();  
        socketChannel.write(buffer);
	}
	
	protected void registerChannel(Selector selector,  
            SelectableChannel channel, int ops) throws Exception {  
        if (channel == null) {  
  
            return; // could happen  
        }  
        // Set the new channel nonblocking  
        channel.configureBlocking(false);  
        // Register it with the selector  
        channel.register(selector, ops);  
    }
	
	private void sayHello(SocketChannel channel) throws Exception {  
		writeToChannel("request received.", channel);
    } 
	
	static class SocketClient implements Serializable, Runnable{
		Pattern pattern;
		Socket socket;
		String id;
		String ip;
		int port;
		LinkedBlockingQueue<PrismScribeLog> queue;
		Thread t;
		
		SocketClient(String ip, int port, String patternStr) throws UnknownHostException, IOException{
			this.pattern = Pattern.compile(patternStr);
			id = patternStr + " " + ip + ":" + port;
			this.ip = ip;
			this.port = port;
			queue = new LinkedBlockingQueue<PrismScribeLog>(10000);
		}
		
		public void write(PrismScribeLog psl){
			if(pattern.matcher(psl.toString()).find()){
				queue.offer(psl);	
			}
		}
		
		public void start() throws UnknownHostException, IOException{
			socket = new Socket(ip, port);
			t = new Thread(this);
			t.start();
		}
		
		public void stop(){
			t.interrupt();
		}
		
		public String toString(){
			return id;
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((id == null) ? 0 : id.hashCode());
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
			SocketClient other = (SocketClient) obj;
			if (id == null) {
				if (other.id != null)
					return false;
			} else if (!id.equals(other.id))
				return false;
			return true;
		}

		@Override
		public void run() {
			try {
				while (true) {
					StringBuilder sb = new StringBuilder(4096);
					for(int i = 0; i < 50; i++){
						PrismScribeLog log = queue.poll();
						if(log == null){
							break;
						}
						sb.append(log.toString()).append("\r\n");
					}
					
					if (sb.length() == 0) {
						Utils.sleep(200);
						continue;
					}
					socket.getOutputStream().write(sb.toString().getBytes());
				}
			} catch(Exception e){
				try {
					socket.close();
				} catch (IOException e1) {
				}
				LOG.error("socket client closed:" + e.getMessage(), e);
			}finally {
				LOG.info("socket client handle complete for:" + id + " the left socket client is:" + socketClients);
				socketClients.remove(this);
			}
		}
	}
	
	public static void main(String[] args){
		new PrismStreamSocketBolt().prepare(null, null);
		Utils.sleep(10000000);
	}
}
