package io.ymq.example.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.List;

//@SpringBootApplication
public class SpringConsumerApplication1 implements CommandLineRunner {
	/**
	 * 消费者的组名
	 */
	@Value("${apache.rocketmq.consumer.PushConsumer}")
	private String consumerGroup;

	/**
	 * NameServer地址
	 */
	@Value("${apache.rocketmq.namesrvAddr}")
	private String namesrvAddr;

	/**
	 * NameServer地址
	 */
	@Value("${apache.rocketmq.topic}")
	private String topic;

	public static void main(String[] args) {
		SpringApplication.run(SpringConsumerApplication1.class, args);
	}

	@Override
	public void run(String... strings) throws Exception {

		//消费者的组名
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);

		//指定NameServer地址，多个地址以 ; 隔开
		consumer.setNamesrvAddr(namesrvAddr);
		try {
			//订阅PushTopic下Tag为push的消息
			consumer.subscribe(topic, "*");

			//设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费
			//如果非第一次启动，那么按照上次消费的位置继续消费
			consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
			consumer.registerMessageListener(new MessageListenerConcurrently() {

				@Override
				public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
					try {
						for (MessageExt messageExt : list) {

							//System.out.println("messageExt: " + messageExt);//输出消息内容

							String messageBody = new String(messageExt.getBody(), "utf-8");

							String tag = "c1";
							System.out.println(tag+ " 消费响应：Msg: " + messageExt.getMsgId() + ",msgBody: " + messageBody);//输出消息内容

						}
					} catch (Exception e) {
						e.printStackTrace();
						return ConsumeConcurrentlyStatus.RECONSUME_LATER; //稍后再试
					}
					return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; //消费成功
				}


			});
			consumer.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
