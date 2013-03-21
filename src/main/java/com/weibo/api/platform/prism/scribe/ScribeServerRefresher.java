package com.weibo.api.platform.prism.scribe;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import backtype.storm.utils.Utils;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;

/**
 * 在每次创建ScribeSpout时，都会经过ScribeReceiver创建一个监听端口，这个监控端口的ip与服务器都会在zk中注册，但scribe日志提供方并不能感知这一变化，因此提供nginx提供一个tcp代理，同时通过zk感知到server发生变化的时候，将变化通过给nginx（修改配置文件），同时reload nginx，以实现配置文件更新。
 * @author crystal
 *
 */
public class ScribeServerRefresher {
	private static final Logger LOG = Logger.getLogger(ScribeServerRefresher.class);
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		if(args == null || args.length < 6){
			LOG.error("Start Usage: java $class $zkStr $prism.scribe.server.path $nginx.conf $nginx.conf.server.patten $ip_idx $reload.cmd");
			System.exit(-1);
		}
		//localhost:2181 /usr/local/nginx/conf/nginx.conf "/usr/local/nginx/sbin/nginx -s reload" /prism/servers/scribe/logcollect   "(.*)server( *)(.*);(.*)prism.scribe.servers(.*)" 3 /prism/servers/tailf "(.*)server( *)(.*);(.*)prism.tailf.servers(.*)" 3
		String zkStr = args[0];
		final String config = args[1];
		String cmd = args[2];

		while(true){
			try{
				boolean changed = false;
				for (int i = 3; i < args.length;) {
					String zkPath = args[i++];
					String serverPattern = args[i++];
					int ipGroupIndex = Integer.parseInt(args[i++]);
					Pattern p = Pattern.compile(serverPattern);
					changed = changed
							|| refresherNginxConfigByZK(zkStr, zkPath, config,
									ipGroupIndex, cmd, p);
				}
				if (changed) {
					reloadNginx(cmd);
				}
			} catch(Exception e){
				LOG.error("refresher nginx config error:" + e.getMessage(), e);
			} finally{
				Utils.sleep(1000);
			}
		}
	}

	/**
	 * @param zkStr
	 * @param path
	 * @param config
	 * @param ipGroupIndex
	 * @param cmd
	 * @param p
	 * @throws Exception 
	 */
	private static boolean refresherNginxConfigByZK(String zkStr,
			final String path, final String config, int ipGroupIndex,
			String cmd, Pattern p) throws Exception {
		final CuratorFramework zk = CuratorFrameworkFactory.newClient(zkStr,
				12000, 6000, new RetryNTimes(4, 1000));
		zk.start();
		try {
			List<String> alivedServers = zk.getChildren().forPath(path);
			if (alivedServers == null || alivedServers.isEmpty()) {
				LOG.warn("No Available Node Registered in zk:" + path);
				Utils.sleep(1000);
				return false;
			}
			List<String> configContents = loadNginxConfig(config);

			Map<Integer, String> configServers = parseConfigServers(
					configContents, p, ipGroupIndex);
			boolean changed = changed(configServers, alivedServers);
			if (changed) {
				List<String> refreshedContents = refreshConfig(configContents,
						alivedServers, configServers);
				flushConfig(refreshedContents, config);
				LOG.info("Nginx Config File Refreshed:" + alivedServers);
			}
			LOG.info("Check Scribe Server Change Complate. the current server is "
					+ (changed ? "changed" : "not changed."));
			return changed;
		} finally {
			zk.close();
		}
	}
	
	private static void reloadNginx(String cmd) throws IOException {
		Runtime.getRuntime().exec(cmd);
	}

	private static void flushConfig(List<String> refreshedContents, String config) throws IOException {
		InputStream is = new FileInputStream(config);
		OutputStream out = new FileOutputStream(config + ".pre");
		IOUtils.copy(is, out);
		is.close();
		out.close();
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(config));
		for(String line : refreshedContents){
			bw.write(line);
			bw.write("\r\n");
		}
		bw.close();
	}

	private static List<String> refreshConfig(List<String> configContents,
			List<String> existsServers, Map<Integer, String> configServers) {
		List<Integer> idxes = new ArrayList<Integer>(configServers.keySet());
		Collections.sort(idxes);
		int start = idxes.get(0);
		int end = idxes.get(idxes.size() - 1);
		
		String template = configContents.get(start);
		String replaceIPPort = configServers.get(start);
		
		List<String> refreshed = new ArrayList<String>();
		refreshed.addAll(configContents.subList(0, start));
		for(String ipPort : existsServers){
			refreshed.add(template.replace(replaceIPPort, ipPort));
		}
		refreshed.addAll(configContents.subList(end + 1, configContents.size()));
		return refreshed;
	}

	private static boolean changed(Map<Integer, String> configServers,
			List<String> existsServers) {
		if(configServers.isEmpty()){
			LOG.warn("No Servers Config in config file.");
			return false;
		}
		List<String> sortConfigServers = new ArrayList<String>(configServers.values());
		Collections.sort(sortConfigServers);
		Collections.sort(existsServers);
		
		return !sortConfigServers.equals(existsServers);
	}

	private static Map<Integer, String> parseConfigServers(
			List<String> configContents, Pattern p, int ipGroupIndex) {
		Map<Integer, String> groups = new HashMap<Integer, String>();
		List<String> refreshedContents = new ArrayList<String>(configContents.size());
		int idx = 0;
		for(String line : configContents){
			Matcher m = p.matcher(line);
			if(!m.matches()){
				refreshedContents.add(line);
			} else {
				groups.put(idx, m.group(ipGroupIndex));
			}
			idx++;
		}
		return groups;
	}

	static List<String> loadNginxConfig(String configFile) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(configFile));
		List<String> lines = IOUtils.readLines(br);
		br.close();
		return lines;
	}
}
