package com.hyf.rabbitmq.client;



import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hyf.rabbitmq.util.EndPoint;
import com.rabbitmq.client.MessageProperties;

/**
 * 客户端(生产者客户端的发送消) 
 */
public class ClientSender extends EndPoint{
	
	private static final Log log = LogFactory.getLog(ClientSender.class);  
	
	public ClientSender(String queueName){
		super(queueName);
	}

	/**
	 * 客户端(生产者客户端的发送消息)
	 * @param message 发送信息
	 */
	public void sendMessage(String message) {
		try {
			if(message == null || "".equals(message.trim())){
				close();
				log.info("发送消息为空......");
				throw new RuntimeException("发送消息不能为空。");
			}
			log.info("开始发送消息......");
			//发布一条消息  
			//消息标记为持久性 ——通过设置 MessageProperties
			channel.basicPublish("", queueName, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
//			channel.basicPublish("", queueName, null, message.getBytes());
			log.info("发送消息成功");
			log.info(queueName + "发送消息的内容:" + message);
			//关闭通道和连接
			close();
		} catch (Exception e) {
			e.printStackTrace();
			log.info(queueName + "发送消息失败的内容:" + message);
			throw new RuntimeException("发送消息失败."+e.getMessage(),e);
		}
	}	

}
