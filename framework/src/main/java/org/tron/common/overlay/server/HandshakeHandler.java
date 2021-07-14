/*
 * Copyright (c) [2016] [ <ether.camp> ]
 * This file is part of the ethereumJ library.
 *
 * The ethereumJ library is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * The ethereumJ library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with the ethereumJ library. If not, see <http://www.gnu.org/licenses/>.
 */

package org.tron.common.overlay.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.spongycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.tron.common.overlay.discover.node.NodeManager;
import org.tron.common.overlay.message.DisconnectMessage;
import org.tron.common.overlay.message.HelloMessage;
import org.tron.common.overlay.message.P2pMessage;
import org.tron.common.overlay.message.P2pMessageFactory;
import org.tron.core.ChainBaseManager;
import org.tron.core.config.args.Args;
import org.tron.core.db.Manager;
import org.tron.core.metrics.MetricsKey;
import org.tron.core.metrics.MetricsUtil;
import org.tron.core.net.peer.PeerConnection;
import org.tron.protos.Protocol.ReasonCode;

/**
 * 处理握手协议的handler
 */
@Slf4j(topic = "net")
@Component
@Scope("prototype")
public class HandshakeHandler extends ByteToMessageDecoder {

  private Channel channel;

  @Autowired
  private NodeManager nodeManager;

  @Autowired
  private ChannelManager channelManager;

  @Autowired
  private Manager manager;

  @Autowired
  private ChainBaseManager chainBaseManager;

  @Autowired
  private FastForward fastForward;

  private byte[] remoteId;

  private P2pMessageFactory messageFactory = new P2pMessageFactory();

  @Autowired
  private SyncPool syncPool;

  @Override
  //客户端与服务端连接成功时触发
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    //log输出远程地址
    logger.info("channel active, {}", ctx.channel().remoteAddress());
    //绑定ChannelHandlerContext
    channel.setChannelHandlerContext(ctx);
    //如果remoteId存在且长度为64位
    if (remoteId.length == 64) {
      //初始化node节点
      channel.initNode(remoteId, ((InetSocketAddress) ctx.channel().remoteAddress()).getPort());
      //发送hello msg 建立p2p连接
      sendHelloMsg(ctx, System.currentTimeMillis());
    }
  }

  @Override
  //收到消息进行解码
  protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out)
      throws Exception {
    byte[] encoded = new byte[buffer.readableBytes()];
    buffer.readBytes(encoded);
    P2pMessage msg = messageFactory.create(encoded);

    logger.info("Handshake receive from {}, {}", ctx.channel().remoteAddress(), msg);

    switch (msg.getType()) {
      case P2P_HELLO:
        handleHelloMsg(ctx, (HelloMessage) msg);
        break;
      case P2P_DISCONNECT:
        if (channel.getNodeStatistics() != null) {
          channel.getNodeStatistics()
              .nodeDisconnectedRemote(((DisconnectMessage) msg).getReasonCode());
        }
        channel.close();
        break;
      default:
        channel.close();
        break;
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    channel.processException(cause);
  }

  public void setChannel(Channel channel, String remoteId) {
    this.channel = channel;
    this.remoteId = Hex.decode(remoteId);
  }

  //发送hello消息
  protected void sendHelloMsg(ChannelHandlerContext ctx, long time) {
    HelloMessage message = new HelloMessage(nodeManager.getPublicHomeNode(), time,
        chainBaseManager.getGenesisBlockId(), chainBaseManager.getSolidBlockId(),
        chainBaseManager.getHeadBlockId());
    fastForward.fillHelloMessage(message, channel);
    ctx.writeAndFlush(message.getSendData());
    channel.getNodeStatistics().messageStatistics.addTcpOutMessage(message);
    MetricsUtil.meterMark(MetricsKey.NET_TCP_OUT_TRAFFIC,
        message.getSendData().readableBytes());
    logger.info("Handshake send to {}, {} ", ctx.channel().remoteAddress(), message);
  }

  //接收处理hello消息
  private void handleHelloMsg(ChannelHandlerContext ctx, HelloMessage msg) {

    //初始化节点
    channel.initNode(msg.getFrom().getId(), msg.getFrom().getPort());

    //校验不通过直接关闭channel
    if (!fastForward.checkHelloMessage(msg, channel)) {
      channel.disconnect(ReasonCode.UNEXPECTED_IDENTITY);
      return;
    }

    //判断remoteId长度  不为64位时必须为可信任的配置节点
    if (remoteId.length != 64) {
      InetAddress address = ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress();
      if (channelManager.getTrustNodes().getIfPresent(address) == null && !syncPool
          .isCanConnect()) {
        channel.disconnect(ReasonCode.TOO_MANY_PEERS);
        return;
      }
    }

    //判断消息的版本和配置的是否一致
    if (msg.getVersion() != Args.getInstance().getNodeP2pVersion()) {
      logger.info("Peer {} different p2p version, peer->{}, me->{}",
          ctx.channel().remoteAddress(), msg.getVersion(), Args.getInstance().getNodeP2pVersion());
      channel.disconnect(ReasonCode.INCOMPATIBLE_VERSION);
      return;
    }

    //判断创世块儿BlockId是否一致
    if (!Arrays
        .equals(chainBaseManager.getGenesisBlockId().getBytes(),
            msg.getGenesisBlockId().getBytes())) {
      logger
          .info("Peer {} different genesis block, peer->{}, me->{}", ctx.channel().remoteAddress(),
              msg.getGenesisBlockId().getString(),
              chainBaseManager.getGenesisBlockId().getString());
      channel.disconnect(ReasonCode.INCOMPATIBLE_CHAIN);
      return;
    }

    //判断如果数据库中最新节点block的num大于等于msg中的num 但是 数据库中不包含msg中的block 则异常返回
    if (chainBaseManager.getSolidBlockId().getNum() >= msg.getSolidBlockId().getNum()
        && !chainBaseManager.containBlockInMainChain(msg.getSolidBlockId())) {
      logger.info("Peer {} different solid block, peer->{}, me->{}", ctx.channel().remoteAddress(),
          msg.getSolidBlockId().getString(), chainBaseManager.getSolidBlockId().getString());
      channel.disconnect(ReasonCode.FORKED);
      return;
    }

    //设置channel的helloMessage消息
    ((PeerConnection) channel).setHelloMessage(msg);

    //统计tcp连接入站出站数量
    channel.getNodeStatistics().messageStatistics.addTcpInMessage(msg);

    //channel处理HelloMessage 握手信息
    channel.publicHandshakeFinished(ctx, msg);

    //如果当前channel不能加入新的活跃节点中 则返回 不建立连接
    if (!channelManager.processPeer(channel)) {
      return;
    }

    //如果remoteId存在且长度为64位 则 发送HelloMsg
    if (remoteId.length != 64) {
      sendHelloMsg(ctx, msg.getTimestamp());
    }

    //当前channel建立连接
    syncPool.onConnect(channel);
  }
}
