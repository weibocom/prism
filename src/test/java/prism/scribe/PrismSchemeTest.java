package prism.scribe;
import java.io.BufferedReader;
import java.io.FileReader;
import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.time.DateUtils;
import org.junit.Test;

import com.weibo.api.platform.prism.storm.core.PrismScheme;
import com.weibo.api.platform.prism.storm.core.ScribeLogScheme;


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

}
