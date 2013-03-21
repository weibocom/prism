package com.weibo.api.platform.prism.storm.scanner;

import java.io.Serializable;

import org.apache.commons.lang.StringUtils;

import com.weibo.api.platform.prism.storm.core.PrismComponentStream;

public class PrismComponentClassFilter implements Serializable, ClassFilter{

	@Override
	public boolean accept(String className) {
		return StringUtils.endsWith(className, ".class");
	}

	@Override
	public boolean accept(Class clazz) {
		if(clazz == null){
			return false;
		}
		return clazz.getAnnotation(PrismComponentStream.class) != null;
	}

}
