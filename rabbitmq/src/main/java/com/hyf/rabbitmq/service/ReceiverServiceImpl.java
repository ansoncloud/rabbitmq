package com.hyf.rabbitmq.service;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hyf.rabbitmq.util.EndPoint;
import com.rabbitmq.client.QueueingConsumer;

/**
 * 读取队列的程序端，实现了Runnable接口。处理凭证数据操作
 * @author 黄永丰
 * @createtime 2015年11月20日
 * @version 1.0
 */
public class ReceiverServiceImpl extends EndPoint implements Runnable
{
	private static final Log log = LogFactory.getLog(ReceiverServiceImpl.class); 
	
	public ReceiverServiceImpl(String queueName) 
	{
		super(queueName);
	}
	
	/**
	 * 在每个线程里执行对应的任务,消费者客户端接收消息
	 * @author 黄永丰
	 * @createtime 2015年11月20日
	 * @version 1.0
	 */
	public void run() 
	{
		String message = null;
		String result = null;
		try 
		{
			log.info(queueName+"开始接收消息......");
			//证在接收端一个消息没有处理完时不会接收另一个消息，即接收端发送了ack后才会接收下一个消息。在这种情况下发送端会尝试把消息发送给下一个not busy的接收端。
    		channel.basicQos(1);
    		QueueingConsumer consumer = new QueueingConsumer(channel);
    		channel.basicConsume(queueName, false, consumer);//autoAck = false ,由下面basicAckd确定是否完成任务，然后才删除队列信息
    		while(true) //死循环 不断接收客户端发送的信息
    		{  
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				message = new String(delivery.getBody());//接收发过来的信息
				if(message != null && !"".equals(message.trim()))
				{
					System.out.println(message);
					//执行完成后才删除队列的任务，处理现金流水记账成功或失败都删除队列当前记录
					channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
				}
    		 }
		} 
		catch (Exception e) 
		{
			log.info(queueName+"队列线程出现异常."+e.getMessage());
		}
	}
	

	
}
