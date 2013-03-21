package com.weibo.api.platform.prism.storm.scanner;

import org.apache.commons.lang.ClassUtils;
import org.apache.log4j.Logger;

import com.weibo.api.platform.prism.storm.spout.ScribeReceiver;

public class ScannerFactory {
	private static final Logger LOG = Logger.getLogger(ScribeReceiver.class);
	
	private static ComponentScanner scanner;
	public static ComponentScanner getScanner() {
		if(scanner == null){
			synchronized (ScannerFactory.class) {
				if(scanner == null){
					String clazz = System.getProperty("prism.component.scanner", PackageComponentScanner.class.getName());
					try {
						scanner = (ComponentScanner)ClassUtils.getClass(clazz).newInstance();
					} catch (Exception e) {
						LOG.error("load prism component scanner failed:" + e.getMessage(), e);
						throw new RuntimeException(e.getMessage(), e);
					}
				}
			}
		}
		return scanner;
	}
}
