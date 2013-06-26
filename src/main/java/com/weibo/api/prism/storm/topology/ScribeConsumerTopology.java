package com.weibo.api.prism.storm.topology;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import com.weibo.api.prism.storm.Constants;
import com.weibo.api.prism.storm.core.PrismComponentStream;
import com.weibo.api.prism.storm.scanner.ComponentScanner;
import com.weibo.api.prism.storm.scanner.ScannerFactory;
import com.weibo.api.prism.storm.spout.ScribeSpout;

public class ScribeConsumerTopology {
	private static final Logger LOG = Logger.getLogger(ScribeConsumerTopology.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if(args == null || args.length < 2){
			LOG.error("Start Usage: java $class $zkStr $prism.scribe.server.path");
			System.exit(-1);
		}
		String zkStr = args[0];
        String scribeServerRegPath = args[1];
        
        clean(zkStr, scribeServerRegPath);
        
		ComponentScanner scanner = ScannerFactory.getScanner();
		List<Class> clazzes = scanner.scan();

		String prismSpoutId = Constants.SPOUT_ID_PRISM_ROOT;
		ScribeSpout ss = new ScribeSpout(prismSpoutId);
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(prismSpoutId, ss, 3);
		
		addComponentToTopology(clazzes, prismSpoutId, builder);
		
		Config conf = new Config();
		conf.setDebug(false);
		conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);//忽略可靠性
//		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1000);
		conf.put("prism.servers.scribe.register", scribeServerRegPath);
        conf.put("prism.zk",zkStr);
		conf.setNumWorkers(6);
		// Topology run
		String mcqComsumerTopologyId = Constants.PRISM_TOPOLOGY;
		StormTopology topology = builder.createTopology();
		
		boolean clusterMode = args != null && args.length > 0 && StringUtils.equals(args[args.length - 1], "cluster");
		
		if(clusterMode){//集群模式
			StormSubmitter.submitTopology(mcqComsumerTopologyId, conf, topology);	
		} else {// 本机模式 用于测试
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(mcqComsumerTopologyId, conf,
					builder.createTopology());
			Utils.sleep(30000);
			cluster.shutdown();
		}
		
		LOG.info("Topology:[" + mcqComsumerTopologyId + "] started in [" + (clusterMode ? "cluster" : "local") + "] mode");
	}
	
	private static void clean(String zkStr, String scribeServerRegPath) {
		CuratorFramework zk;
		try {
			zk = CuratorFrameworkFactory.newClient(
			        zkStr,
			        1500,
			        1500,
			        new RetryNTimes(3, 1000));
	        zk.start();
	        List<String> children = zk.getChildren().forPath(scribeServerRegPath);
	        for(String child : children){
	        	zk.delete().forPath(scribeServerRegPath + "/" + child);
	        }
	        zk.close();
		} catch (Exception e) {
			LOG.error("failed to clean " + scribeServerRegPath + " on " + zkStr, e);
		}

	}

	private static void addComponentToTopology(List<Class> clazzes,
			String parentId, TopologyBuilder builder) throws Exception {
		List<Class> subComponents = ScannerFactory.getScanner().children(parentId);
		for(Class c : subComponents){
			Object o = c.newInstance();
			if(!(o instanceof IBasicBolt || o instanceof IRichBolt)){
				LOG.warn(c + " must be an instanceof (" + IBasicBolt.class + " or " + IRichBolt.class + ")");
				continue;
			}
			PrismComponentStream pc = (PrismComponentStream)c.getAnnotation(PrismComponentStream.class);
			String id = pc.id();
			int parallelism = pc.parallelism();
			BoltDeclarer bd;
			if(o instanceof IBasicBolt){
				bd = builder.setBolt(id, (IBasicBolt)o, parallelism);
			} else {
				bd = builder.setBolt(id, (IRichBolt)o, parallelism);
			}
			
			switch (pc.groupingPolocy()) {
			case FIELDS:
				bd.fieldsGrouping(pc.source(), pc.stream(), new Fields(pc.groupingField()));
				break;
			case SHUFFLE:
				bd.shuffleGrouping(pc.source(), pc.stream());
				break;
			case GLOABLE:
				bd.globalGrouping(pc.source(), pc.stream());
				break;
			default:
				LOG.warn("unkown grouping polocy:" + pc.groupingPolocy());
			}
			addComponentToTopology(clazzes, id, builder);
		}
	}
}
