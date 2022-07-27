package org.tron.common.overlay.server;

import static org.tron.protos.Protocol.ReasonCode.DUPLICATE_PEER;
import static org.tron.protos.Protocol.ReasonCode.TOO_MANY_PEERS;
import static org.tron.protos.Protocol.ReasonCode.TOO_MANY_PEERS_WITH_SAME_IP;
import static org.tron.protos.Protocol.ReasonCode.UNKNOWN;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.common.overlay.client.PeerClient;
import org.tron.common.overlay.discover.node.Node;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.prometheus.MetricKeys;
import org.tron.common.prometheus.Metrics;
import org.tron.core.config.args.Args;
import org.tron.core.db.ByteArrayWrapper;
import org.tron.core.metrics.MetricsKey;
import org.tron.core.metrics.MetricsUtil;
import org.tron.protos.Protocol;
import org.tron.protos.Protocol.ReasonCode;

@Slf4j(topic = "net")
@Component
public class ChannelManager {

  /**
   * 握手成功加入activePeers       ChannelManager(190)
   * ChannelManager.activePeers.size = SyncPool.activePeersCount + SyncPool.passivePeersCount
   */
  private final Map<ByteArrayWrapper, Channel> activePeers = new ConcurrentHashMap<>();
  @Autowired
  private PeerServer peerServer;
  @Autowired
  private PeerClient peerClient;
  @Autowired
  private SyncPool syncPool;
  @Autowired
  private FastForward fastForward;
  private CommonParameter parameter = CommonParameter.getInstance();
  private Cache<InetAddress, ReasonCode> badPeers = CacheBuilder.newBuilder().maximumSize(10000)
      .expireAfterWrite(1, TimeUnit.HOURS).recordStats().build();

  private Cache<InetAddress, ReasonCode> recentlyDisconnected = CacheBuilder.newBuilder()
      .maximumSize(1000).expireAfterWrite(30, TimeUnit.SECONDS).recordStats().build();

  //握手成功 24h 缓存
  @Getter
  private Cache<String, Protocol.HelloMessage> helloMessageCache = CacheBuilder.newBuilder()
          .maximumSize(2000).expireAfterWrite(24, TimeUnit.HOURS).recordStats().build();

  /**
   * 1. TCP连接满时不会被断开连接
   * 2. 建立连接(handleHelloMsg)时不受recentlyDisconnected，badPeers，TOO_MANY_PEERS，TOO_MANY_PEERS_WITH_SAME_IP的影响
   * 3. PassiveNodes  ActiveNodes  FastForwardNodes 均加入 trustNodes
   */
  @Getter
  private Cache<InetAddress, Node> trustNodes = CacheBuilder.newBuilder().maximumSize(100).build();

  /**
   * 1. 配置文件中的ActiveNodes
   * 2. FastForwardNodes节点
   */
  @Getter
  private Map<InetAddress, Node> activeNodes = new ConcurrentHashMap();

  /**
   * 1. 接收处理区块过程中，快速转发节点不校验 block请求，block大小，接收block时间  BlockMsgHandler(63)
   * 2. 广播过程汇总，快速转发节点不广播交易，只广播区块，走fastsend 直接发送
   * AdvService.sendInv(375)   BlockMsgHandler.broadcast(155)
   * 3. 中心转发节点不接收交易，因为不发送交易的 FETCH_ENV_DATA 消息  AdvService.sendInv(358)
   * 4. 产块的SR节点握手时 会主动连接 fastForwardNodes节点      FastForward.fillHelloMessage(87)
   * 5. 中心转发节点只能连接正在产块的SR节点(收到握手消息时校验)    HandshakeHandler(160) FastForward.checkHelloMessage(105)
   * 6. fastforward节点 不执行节点发现逻辑  DiscoverServer !parameter.isFastForward(65)
   * 7. fastforward节点广播区块时，会先向后3个产块的SR节点广播区块  RelayService.broadcast.witnesses(48)
   */
  @Getter
  private Map<InetAddress, Node> fastForwardNodes = new ConcurrentHashMap();

  private int maxActivePeers = parameter.getNodeMaxActiveNodes();

  private int getMaxActivePeersWithSameIp = parameter.getNodeMaxActiveNodesWithSameIp();

  public void init() {
    if (this.parameter.getNodeListenPort() > 0) {
      new Thread(() -> peerServer.start(Args.getInstance().getNodeListenPort()),
          "PeerServerThread").start();
    }

    InetAddress address;
    for (Node node : parameter.getPassiveNodes()) {
      address = new InetSocketAddress(node.getHost(), node.getPort()).getAddress();
      trustNodes.put(address, node);
    }

    for (Node node : parameter.getActiveNodes()) {
      address = new InetSocketAddress(node.getHost(), node.getPort()).getAddress();
      trustNodes.put(address, node);
      activeNodes.put(address, node);
    }

    for (Node node : parameter.getFastForwardNodes()) {
      address = new InetSocketAddress(node.getHost(), node.getPort()).getAddress();
      trustNodes.put(address, node);
      fastForwardNodes.put(address, node);
    }

    logger.info("Node config, trust {}, active {}, forward {}.",
        trustNodes.size(), activeNodes.size(), fastForwardNodes.size());

    syncPool.init();
    fastForward.init();
  }

  public void processDisconnect(Channel channel, ReasonCode reason) {
    InetAddress inetAddress = channel.getInetAddress();
    if (inetAddress == null) {
      return;
    }
    switch (reason) {
      case BAD_PROTOCOL:
      case BAD_BLOCK:
      case BAD_TX:
        badPeers.put(channel.getInetAddress(), reason);
        break;
      default:
        recentlyDisconnected.put(channel.getInetAddress(), reason);
        break;
    }
    MetricsUtil.counterInc(MetricsKey.NET_DISCONNECTION_COUNT);
    MetricsUtil.counterInc(MetricsKey.NET_DISCONNECTION_DETAIL + reason);
    Metrics.counterInc(MetricKeys.Counter.P2P_DISCONNECT, 1,
        reason.name().toLowerCase(Locale.ROOT));
  }

  public void notifyDisconnect(Channel channel) {
    syncPool.onDisconnect(channel);
    activePeers.values().remove(channel);
    if (channel != null) {
      if (channel.getNodeStatistics() != null) {
        channel.getNodeStatistics().notifyDisconnect();
      }
      InetAddress inetAddress = channel.getInetAddress();
      if (inetAddress != null && recentlyDisconnected.getIfPresent(inetAddress) == null) {
        recentlyDisconnected.put(channel.getInetAddress(), UNKNOWN);
      }
    }
  }

  public synchronized boolean processPeer(Channel peer) {

    if (trustNodes.getIfPresent(peer.getInetAddress()) == null) {
      if (recentlyDisconnected.getIfPresent(peer) != null) {
        logger.info("Peer {} recently disconnected.", peer.getInetAddress());
        return false;
      }

      if (badPeers.getIfPresent(peer) != null) {
        peer.disconnect(peer.getNodeStatistics().getDisconnectReason());
        return false;
      }

      if (!peer.isActive() && activePeers.size() >= maxActivePeers) {
        peer.disconnect(TOO_MANY_PEERS);
        return false;
      }

      if (getConnectionNum(peer.getInetAddress()) >= getMaxActivePeersWithSameIp) {
        peer.disconnect(TOO_MANY_PEERS_WITH_SAME_IP);
        return false;
      }
    }

    Channel channel = activePeers.get(peer.getNodeIdWrapper());
    //重复建立连接 断开比较晚的连接
    if (channel != null) {
      //如果新连接的时间早于已有连接 则断开已有连接，加入新链接
      if (channel.getStartTime() > peer.getStartTime()) {
        logger.info("Disconnect connection established later, {}", channel.getNode());
        channel.disconnect(DUPLICATE_PEER);
      } else {
        //如果新连接的时间晚于已有连接 则断开新链接 直接返回
        peer.disconnect(DUPLICATE_PEER);
        return false;
      }
    }
    //握手成功加入activePeers
    activePeers.put(peer.getNodeIdWrapper(), peer);
    logger.info("Add active peer {}, total active peers: {}", peer, activePeers.size());
    return true;
  }

  public int getConnectionNum(InetAddress inetAddress) {
    int cnt = 0;
    for (Channel channel : activePeers.values()) {
      if (channel.getInetAddress().equals(inetAddress)) {
        cnt++;
      }
    }
    return cnt;
  }

  public Collection<Channel> getActivePeers() {
    return activePeers.values();
  }

  public Cache<InetAddress, ReasonCode> getRecentlyDisconnected() {
    return this.recentlyDisconnected;
  }

  public Cache<InetAddress, ReasonCode> getBadPeers() {
    return this.badPeers;
  }

  public void close() {
    syncPool.close();
    peerServer.close();
    peerClient.close();
  }
}
