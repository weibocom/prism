package com.weibo.api.prism.storm.core;


public interface PrismScribeLogFilter {
	boolean accept(PrismScribeLog psl);
}
