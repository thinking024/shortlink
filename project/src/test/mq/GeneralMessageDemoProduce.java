package mq;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class GeneralMessageDemoProduce {

    private final RocketMQTemplate rocketMQTemplate;

    /**
     * 发送普通消息
     *
     * @param topic            消息发送主题，用于标识同一类业务逻辑的消息
     * @param tag              消息的过滤标签，消费者可通过Tag对消息进行过滤，仅接收指定标签的消息。
     * @param keys             消息索引键，可根据关键字精确查找某条消息
     * @param messageSendEvent 普通消息发送事件，自定义对象，最终都会序列化为字符串
     * @return 消息发送 RocketMQ 返回结果
     */
    public SendResult sendMessage(String topic, String tag, String keys, GeneralMessageEvent messageSendEvent) {
        SendResult sendResult;
        try {
            StringBuilder destinationBuilder = StrUtil.builder().append(topic);
            if (StrUtil.isNotBlank(tag)) {
                destinationBuilder.append(":").append(tag);
            }
            Message<?> message = MessageBuilder
                    .withPayload(messageSendEvent)
                    .setHeader(MessageConst.PROPERTY_KEYS, keys)
                    .setHeader(MessageConst.PROPERTY_TAGS, tag)
                    .build();
            sendResult = rocketMQTemplate.syncSend(
                    destinationBuilder.toString(),
                    message,
                    2000L
            );
            log.info("[普通消息] 消息发送结果：{}，消息ID：{}，消息Keys：{}", sendResult.getSendStatus(), sendResult.getMsgId(), keys);
        } catch (Throwable ex) {
            log.error("[普通消息] 消息发送失败，消息体：{}", JSON.toJSONString(messageSendEvent), ex);
            throw ex;
        }
        return sendResult;
    }
}
