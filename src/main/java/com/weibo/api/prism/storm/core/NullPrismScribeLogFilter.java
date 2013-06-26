package com.weibo.api.prism.storm.core;

import java.io.Serializable;

public class NullPrismScribeLogFilter implements PrismScribeLogFilter,
		Serializable {
	private static final long serialVersionUID = 2276509074172831846L;

	@Override
	public boolean accept(PrismScribeLog psl) {
		if(psl == null){
			return false;
		}
		return true;
	}

}
