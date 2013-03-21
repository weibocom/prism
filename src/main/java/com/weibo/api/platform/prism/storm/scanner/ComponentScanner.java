package com.weibo.api.platform.prism.storm.scanner;

import java.util.List;

import com.weibo.api.platform.prism.storm.core.PrismComponentStream;

public interface ComponentScanner {
	List<Class> scan();
	
	List<Class> children(String parentComponentId);
	
	List<Class> scan(PrismComponentStreamFilter pcsf);
	
	interface PrismComponentStreamFilter {
		boolean accept(PrismComponentStream pcs);
	}
	
	PrismComponentStreamFilter acceptAll = new PrismComponentStreamFilter(){

		@Override
		public boolean accept(PrismComponentStream pcs) {
			return true;
		}
		
	};
}
