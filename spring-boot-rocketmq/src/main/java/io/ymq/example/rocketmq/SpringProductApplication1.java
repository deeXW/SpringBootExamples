package io.ymq.example.rocketmq;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringProductApplication1 implements CommandLineRunner {

	/**
	 * 生产者的组名
	 */
	@Value("${apache.rocketmq.producer.producerGroup}")
	private String producerGroup;

	/**
	 * NameServer 地址
	 */
	@Value("${apache.rocketmq.namesrvAddr}")
	private String namesrvAddr;

	/**
	 * NameServer地址
	 */
	@Value("${apache.rocketmq.topic}")
	private String topic;

	public static void main(String[] args) {
		SpringApplication.run(SpringProductApplication1.class, args);
	}

	@Override
	public void run(String... strings) throws Exception {
		{

			//生产者的组名
			DefaultMQProducer producer = new DefaultMQProducer(producerGroup);

			//指定NameServer地址，多个地址以 ; 隔开
			producer.setNamesrvAddr(namesrvAddr);

			try {

				/**
				 * Producer对象在使用之前必须要调用start初始化，初始化一次即可
				 * 注意：切记不可以在每次发送消息时，都调用start方法
				 */
				producer.start();

				for (int i = 0; i < 10; i++) {

					String messageBody = "我是消息内容:" + i;

					String message = new String(messageBody.getBytes(), "utf-8");

					//构建消息
					Message msg = new Message(topic, "*", "key_" + i /* Keys */, message.getBytes());

					//发送消息
					SendResult result = producer.send(msg);

					System.out.println("发送响应：MsgId:" + result.getMsgId() + "，发送状态:" + result.getSendStatus());

				}

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				producer.shutdown();
			}

		}
	}
}
