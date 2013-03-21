package com.weibo.api.platform.prism.storm.core;


public interface PrismScribeLogFilter {
	boolean accept(PrismScribeLog psl);
}
