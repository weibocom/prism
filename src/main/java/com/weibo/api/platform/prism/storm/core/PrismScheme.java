package com.weibo.api.platform.prism.storm.core;


public interface PrismScheme {
	public PrismScribeLog deserialize(String msg);
}
