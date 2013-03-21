package com.weibo.api.platform.prism.storm.scanner;

public interface ClassFilter {
	public boolean accept(String className);
	
	public boolean accept(Class clazz);
}
