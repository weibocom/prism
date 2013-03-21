package com.weibo.api.platform.prism.storm.scanner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.weibo.api.platform.prism.storm.core.PrismComponentStream;

public class PackageComponentScanner implements ComponentScanner {
	private static final Logger LOG = Logger.getLogger(PackageComponentScanner.class);
	
	private static List<Class> components;
	
	private static boolean scanned;
	
	private static ClassPathScanHandler cph = new ClassPathScanHandler(true, new PrismComponentClassFilter());

	@Override
	public List<Class> scan() {
		return scan(ComponentScanner.acceptAll);
	}

	@Override
	public List<Class> children(final String parentComponentId) {
		return scan(new PrismComponentStreamFilter() {
			@Override
			public boolean accept(PrismComponentStream pcs) {
				if(pcs == null){
					return false;
				}
				return StringUtils.equals(pcs.source(), parentComponentId);
			}
		});
	}

	@Override
	public List<Class> scan(PrismComponentStreamFilter pcsf) {
		if(!scanned){
			synchronized (PackageComponentScanner.class) {
				if(!scanned){
					components = new ArrayList<Class>(cph.getPackageAllClasses("com.weibo.api.platform", true));
					scanned = true;
				}
			}
		}
		List<Class> scs = new ArrayList<Class>(components.size());
		for(Class c : components){
			if(pcsf.accept((PrismComponentStream)c.getAnnotation(PrismComponentStream.class))){
				scs.add(c);
			}
		}
		return scs;
	}

}
