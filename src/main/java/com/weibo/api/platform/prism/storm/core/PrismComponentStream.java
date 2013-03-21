package com.weibo.api.platform.prism.storm.core;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import backtype.storm.utils.Utils;

import com.weibo.api.platform.prism.storm.Constants;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface PrismComponentStream {
	String id() default ""; // 当前组件的名称，如果为空，则默认为被注解类的全名
	
	String source() default Constants.SPOUT_ID_PRISM_ROOT;// 数据源的id。标准化后的数据中心获取
	
	String stream() default Utils.DEFAULT_STREAM_ID; //定义组件订阅的流的id。
	
	Class logFilter() default NullPrismScribeLogFilter.class;// 定义业务需要哪些消息，默认需要所有的消息
	
	int parallelism() default 1; // 需要多少个任务分析数据。
	
	StreamGroupingPolocy groupingPolocy() default StreamGroupingPolocy.SHUFFLE; // 默认为随机策略
	
	String groupingField() default ""; // 如果 groupingPolocy 为 FIELDS时，需要指定按哪个field进行group
	
	static enum StreamGroupingPolocy {
		FIELDS,
		SHUFFLE,
		GLOABLE;
	}
}
