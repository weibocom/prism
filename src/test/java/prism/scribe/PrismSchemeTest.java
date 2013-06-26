package prism.scribe;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.time.DateUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import backtype.storm.utils.Utils;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import com.weibo.api.prism.storm.core.PrismScheme;
import com.weibo.api.prism.storm.core.ScribeLogScheme;


public class PrismSchemeTest {

	@Test
	public void test() throws Exception {
		BufferedReader br = new BufferedReader(new FileReader("/Users/crystal/tmp/prism.log"));
		String line;
		PrismScheme ps = new ScribeLogScheme();
		while((line = br.readLine()) != null){
			System.out.println(ps.deserialize(line));
		}
		br.close();
	}
	
	@Test
	public void classpath(){
		System.out.println(System.getProperties());
	}
	
	@Test
	public void parseServers(){
		String config = "server 10.229.13.87:2004; #prism.scribe.servers";
		Pattern p = Pattern.compile("(.*)server( *)(.*);(.*)prism.scribe.servers(.*)");
		Matcher m = p.matcher(config);
		System.out.println(m.matches());
		for(int i = 1; i <= m.groupCount(); i++){
			System.out.println(m.group(i));	
		}
	}
	
	@Test
	public void fullClassName(){
		System.out.println(PrismSchemeTest.class.getName());
	}
	
	@Test
	public void parseDate(){
			try {
				System.out.println(DateUtils.parseDate("2013-01-08 21:22:22", new String[]{"yyyy-MM-dd HH:mm:ss"}));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	
	@Test
	public void testCurator() throws Exception{
		CuratorFramework    client = CuratorFrameworkFactory.builder().connectString("localhost:2181").connectionTimeoutMs(5000).retryPolicy(new RetryNTimes(10,3000)).sessionTimeoutMs(3000).build();
		client.start();
		List<String> a = client.getChildren().usingWatcher(new Watcher(){

			@Override
			public void process(WatchedEvent event) {
				System.out.println(event.getPath());
				
			}}).forPath("/prism/servers/scribe/register");
		System.out.println(a);
		Utils.sleep(1000000);
	}
	
	@Test
	public void testZK() throws Exception{
		ZooKeeper zk = new ZooKeeper("localhost:2181", 3000, new Watcher(){

			@Override
			public void process(WatchedEvent event) {
				System.out.println(event.getPath());
				
			}});
		System.out.println(zk.getChildren("/prism/servers/scribe/register", true));
		Utils.sleep(1000000);
	}

}
