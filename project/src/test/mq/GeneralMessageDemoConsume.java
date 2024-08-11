package mq;

import com.alibaba.fastjson.JSON;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = "rocketmq-demo_common-message_topic",
        selectorExpression = "general",
        consumerGroup = "rocketmq-demo_general-message_cg"
)
public class GeneralMessageDemoConsume implements RocketMQListener<GeneralMessageEvent> {

    @Override
    public void onMessage(GeneralMessageEvent message) {
        log.info("接到到RocketMQ消息，消息体：{}", JSON.toJSONString(message));
    }
}
