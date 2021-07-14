package org.tron.common.overlay.server;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.tron.common.overlay.message.Message;
import org.tron.common.overlay.message.PingMessage;
import org.tron.common.overlay.message.PongMessage;
import org.tron.consensus.pbft.message.PbftBaseMessage;
import org.tron.core.metrics.MetricsKey;
import org.tron.core.metrics.MetricsUtil;
import org.tron.core.net.message.InventoryMessage;
import org.tron.core.net.message.TransactionsMessage;
import org.tron.protos.Protocol.Inventory.InventoryType;
import org.tron.protos.Protocol.ReasonCode;

/**
 * 消息队列Queue
 */
@Slf4j(topic = "net")
@Component
@Scope("prototype")
public class MessageQueue {

  private static ScheduledExecutorService sendTimer =
      Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "sendTimer"));
  private volatile boolean sendMsgFlag = false;
  private volatile long sendTime;
  private volatile long sendPing;
  private Thread sendMsgThread;
  private Channel channel;
  private ChannelHandlerContext ctx = null;
  private Queue<MessageRoundTrip> requestQueue = new ConcurrentLinkedQueue<>();
  private BlockingQueue<Message> msgQueue = new LinkedBlockingQueue<>();
  private ScheduledFuture<?> sendTask;

  //启动消费队列的线程池
  public void activate(ChannelHandlerContext ctx) {

    this.ctx = ctx;

    sendMsgFlag = true;

    //启动消费requestQueue 的定时任务单线程
    sendTask = sendTimer.scheduleAtFixedRate(() -> {
      try {
        if (sendMsgFlag) {
          send();
        }
      } catch (Exception e) {
        logger.error("Unhandled exception", e);
      }
    }, 10, 10, TimeUnit.MILLISECONDS);

    //启动异步线程消费msgQueue
    sendMsgThread = new Thread(() -> {
      while (sendMsgFlag) {
        try {
          if (msgQueue.isEmpty()) {
            Thread.sleep(10);
            continue;
          }
          Message msg = msgQueue.take();
          ctx.writeAndFlush(msg.getSendData()).addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess() && !channel.isDisconnect()) {
              logger.error("Failed to send to {}, {}", ctx.channel().remoteAddress(), msg);
            }
          });
        } catch (Exception e) {
          logger.error("Failed to send to {}, error info: {}", ctx.channel().remoteAddress(),
              e.getMessage());
        }
      }
    });
    //设置线程名称并启动
    sendMsgThread.setName("sendMsgThread-" + ctx.channel().remoteAddress());
    sendMsgThread.start();
  }

  public void setChannel(Channel channel) {
    this.channel = channel;
  }

  //直接发送
  public void fastSend(Message msg) {
    logger.info("Fast send to {}, {} ", ctx.channel().remoteAddress(), msg);
    ctx.writeAndFlush(msg.getSendData()).addListener((ChannelFutureListener) future -> {
      if (!future.isSuccess() && !channel.isDisconnect()) {
        logger.error("Fast send to {} failed, {}", ctx.channel().remoteAddress(), msg);
      }
    });
  }

  //发送tcp消息到队列中
  public boolean sendMessage(Message msg) {
    long now = System.currentTimeMillis();
    //ping消息 1分钟内最多发送1次  不能距上次发送消息时间小于10s
    if (msg instanceof PingMessage) {
      if (now - sendTime < 10_000 && now - sendPing < 60_000) {
        return false;
      }
      sendPing = now;
    }
    //消息记录日志
    if (needToLog(msg)) {
      logger.info("Send to {}, {} ", ctx.channel().remoteAddress(), msg);
    }

    //添加统计数据
    channel.getNodeStatistics().messageStatistics.addTcpOutMessage(msg);
    //添加业务监控代码
    MetricsUtil.meterMark(MetricsKey.NET_TCP_OUT_TRAFFIC, msg.getSendData().readableBytes());

    sendTime = System.currentTimeMillis();
    //根据不同的AnswerMessage 放入到不同的Queue里
    if (msg.getAnswerMessage() != null) {
      requestQueue.add(new MessageRoundTrip(msg));
    } else {
      msgQueue.offer(msg);
    }
    return true;
  }

  //收到消息 各种指标统计
  public void receivedMessage(Message msg) {
    if (needToLog(msg)) {
      logger.info("Receive from {}, {}", ctx.channel().remoteAddress(), msg);
    }
    channel.getNodeStatistics().messageStatistics.addTcpInMessage(msg);
    MessageRoundTrip rt = requestQueue.peek();
    if (rt != null && rt.getMsg().getAnswerMessage() == msg.getClass()) {
      requestQueue.remove();
      if (rt.getMsg() instanceof PingMessage) {
        channel.getNodeStatistics().pingMessageLatency
            .add(System.currentTimeMillis() - rt.getTime());
      }
    }
  }

  //关闭线程池
  public void close() {
    sendMsgFlag = false;
    if (sendTask != null && !sendTask.isCancelled()) {
      sendTask.cancel(false);
      sendTask = null;
    }
    if (sendMsgThread != null) {
      try {
        sendMsgThread.join(20);
        sendMsgThread = null;
      } catch (Exception e) {
        logger.warn("Join send thread failed, peer {}", ctx.channel().remoteAddress());
      }
    }
  }

  //ping pong 交易 和 共识 消息不记录日志
  private boolean needToLog(Message msg) {
    if (msg instanceof PingMessage
        || msg instanceof PongMessage
        || msg instanceof TransactionsMessage
        || msg instanceof PbftBaseMessage) {
      return false;
    }

    if (msg instanceof InventoryMessage
        && ((InventoryMessage) msg).getInventoryType().equals(InventoryType.TRX)) {
      return false;
    }

    return true;
  }

  //消费requestQueue队列数据
  private void send() {
    MessageRoundTrip rt = requestQueue.peek();
    if (!sendMsgFlag || rt == null) {
      return;
    }
    if (rt.getRetryTimes() > 0 && !rt.hasToRetry()) {
      return;
    }
    if (rt.getRetryTimes() > 0) {
      channel.getNodeStatistics().nodeDisconnectedLocal(ReasonCode.PING_TIMEOUT);
      logger.warn("Wait {} timeout. close channel {}.",
          rt.getMsg().getAnswerMessage(), ctx.channel().remoteAddress());
      channel.close();
      return;
    }

    Message msg = rt.getMsg();

    ctx.writeAndFlush(msg.getSendData()).addListener((ChannelFutureListener) future -> {
      if (!future.isSuccess()) {
        logger.error("Fail send to {}, {}", ctx.channel().remoteAddress(), msg);
      }
    });

    rt.incRetryTimes();
    rt.saveTime();
  }

}
