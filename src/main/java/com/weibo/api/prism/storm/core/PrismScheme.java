package com.weibo.api.prism.storm.core;


public interface PrismScheme {
	public PrismScribeLog deserialize(String msg);
}
