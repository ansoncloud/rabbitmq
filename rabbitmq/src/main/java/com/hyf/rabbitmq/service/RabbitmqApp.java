package com.hyf.rabbitmq.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hyf.rabbitmq.service.ReceiverServiceImpl;
import com.hyf.rabbitmq.util.EndPoint;

/**
 * 消费者 接收信息
 * @author 黄永丰
 * @createtime 2015年11月20日
 * @version 1.0
 */
public class RabbitmqApp {
	
	private static final Log log = LogFactory.getLog(RabbitmqApp.class); 

	public static void main(String[] args) {
		log.info("---------------start rabbitmq-client......---------------------");
		// 接收
		for (int i = 0; i < EndPoint.THREAD_NUM; i++) {
			ReceiverServiceImpl cr = new ReceiverServiceImpl("queueName" + i);
			Thread crThread = new Thread(cr);
			crThread.setName("queueName" + i);
			crThread.start();
		}
		log.info("---------------start rabbitmq-client sucessful---------------------");
	}

}
