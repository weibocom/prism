package com.weibo.api.platform.prism.test;

import java.io.Serializable;

import com.weibo.api.platform.prism.storm.core.PrismScribeLog;
import com.weibo.api.platform.prism.storm.core.PrismScribeLogFilter;

public class TestPrismScribeLogFilter implements PrismScribeLogFilter,
		Serializable {

	@Override
	public boolean accept(PrismScribeLog psl) {
		return psl.getDate().getTime() / 1000 % 10 == 0;
	}

}
