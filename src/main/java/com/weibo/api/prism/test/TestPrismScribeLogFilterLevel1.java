package com.weibo.api.prism.test;

import java.io.Serializable;

import com.weibo.api.prism.storm.core.PrismScribeLog;
import com.weibo.api.prism.storm.core.PrismScribeLogFilter;

public class TestPrismScribeLogFilterLevel1 implements PrismScribeLogFilter,
		Serializable {

	@Override
	public boolean accept(PrismScribeLog psl) {
		return psl.getDate().getTime() / 1000 % 10 != 0;
	}

}
