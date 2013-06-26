package com.weibo.api.tutorial.feed;

import org.apache.commons.lang.StringUtils;

import com.weibo.api.prism.storm.core.PrismScribeLog;
import com.weibo.api.prism.storm.core.PrismScribeLogFilter;

public class FeedMonitorPrismScribeLogFilter implements PrismScribeLogFilter {

	@Override
	public boolean accept(PrismScribeLog psl) {
		if(psl == null){
			return true;
		}
		return StringUtils.equalsIgnoreCase(psl.getModule(), "feedMonitor") && 
				StringUtils.equalsIgnoreCase(psl.getSchema(), "visitStas"); 
	}

}
