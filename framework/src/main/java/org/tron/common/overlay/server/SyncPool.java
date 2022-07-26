package org.tron.common.overlay.server;

import com.codahale.metrics.Snapshot;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.tron.common.overlay.client.PeerClient;
import org.tron.common.overlay.discover.node.NodeHandler;
import org.tron.common.overlay.discover.node.NodeManager;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.prometheus.MetricKeys;
import org.tron.common.prometheus.MetricLabels;
import org.tron.common.prometheus.Metrics;
import org.tron.core.ChainBaseManager;
import org.tron.core.config.args.Args;
import org.tron.core.metrics.MetricsKey;
import org.tron.core.metrics.MetricsUtil;
import org.tron.core.net.peer.PeerConnection;
import org.tron.protos.Protocol;

@Slf4j(topic = "net")
@Component
public class SyncPool {

  /**
   * 主动和被动建立的连接 均加入activePeers   onConnect++   onDisconnect--
   */
  private final List<PeerConnection> activePeers = Collections
      .synchronizedList(new ArrayList<>());
  //被动建立连接的数量   onConnect++   onDisconnect--
  private final AtomicInteger passivePeersCount = new AtomicInteger(0);
  //主动建立连接的数量(通过PeerClient)  onConnect++   onDisconnect--
  private final AtomicInteger activePeersCount = new AtomicInteger(0);
  //默认0.3
  private double factor = Args.getInstance().getConnectFactor();
  //默认0.1
  private double activeFactor = Args.getInstance().getActiveConnectFactor();
  //主动建立的链接加入缓存 180秒不会重复发起TCP连接，避免发起无效的连接请求
  private Cache<NodeHandler, Long> nodeHandlerCache = CacheBuilder.newBuilder()
      .maximumSize(1000).expireAfterWrite(180, TimeUnit.SECONDS).recordStats().build();

  @Autowired
  private NodeManager nodeManager;

  @Autowired
  private ApplicationContext ctx;

  @Autowired
  private ChainBaseManager chainBaseManager;

  private ChannelManager channelManager;

  private CommonParameter commonParameter = CommonParameter.getInstance();

  //最大活跃连接数 默认30
  private int maxActiveNodes = commonParameter.getNodeMaxActiveNodes();

  //默认2
  private int maxActivePeersWithSameIp = commonParameter.getNodeMaxActiveNodesWithSameIp();

  private ScheduledExecutorService poolLoopExecutor = Executors.newSingleThreadScheduledExecutor();

  private ScheduledExecutorService logExecutor = Executors.newSingleThreadScheduledExecutor();

  /**
   * 主动建立远程连接时使用PeerClient  remoteId为对方的nodeid
   * 建立连接时   activePeersCount ++
   * 建立Tcp Channel成功，remoteId不为空时，activePeersCount ++；否则 passivePeersCount ++
   */
  private PeerClient peerClient;

  private int disconnectTimeout = 60_000;

  public void init() {

    channelManager = ctx.getBean(ChannelManager.class);

    peerClient = ctx.getBean(PeerClient.class);

    //维护连接池
    poolLoopExecutor.scheduleWithFixedDelay(() -> {
      try {
        check();
        fillUp();
      } catch (Throwable t) {
        logger.error("Exception in sync worker", t);
      }
    }, 100, 3600, TimeUnit.MILLISECONDS);

    //打印连接状态日志
    logExecutor.scheduleWithFixedDelay(() -> {
      try {
        logActivePeers();
      } catch (Throwable t) {
        logger.error("Exception in sync worker", t);
      }
    }, 30, 10, TimeUnit.SECONDS);
  }

  //检查提出 disconnect 连接   重置分数优先级
  private void check() {
    for (PeerConnection peer : new ArrayList<>(activePeers)) {
      long now = System.currentTimeMillis();
      long disconnectTime = peer.getDisconnectTime();
      if (disconnectTime != 0 && now - disconnectTime > disconnectTimeout) {
        logger.warn("Notify disconnect peer {}.", peer.getInetAddress());
        channelManager.notifyDisconnect(peer);
      }
    }
  }

  private void fillUp() {
    List<NodeHandler> connectNodes = new ArrayList<>();
    Set<InetAddress> addressInUse = new HashSet<>();
    Set<String> nodesInUse = new HashSet<>();
    channelManager.getActivePeers().forEach(channel -> {
      nodesInUse.add(channel.getPeerId());
      addressInUse.add(channel.getInetAddress());
    });

    /**
     * 找出在ActiveNodes中但不在ActivePeers中的node优先加载(优先加载配置文件中的节点)
     */
    channelManager.getActiveNodes().forEach((address, node) -> {
      nodesInUse.add(node.getHexId());
      if (!addressInUse.contains(address)) {
        connectNodes.add(nodeManager.getNodeHandler(node));
      }
    });

    /**
     * maxActiveNodes * factor   最小建立连接的数量
     * maxActiveNodes * activeFactor  最小建立主动连接的数量
     * 选取比较大的值 = 还需要建立连接的树龄
     */
    int size = Math.max((int) (maxActiveNodes * factor) - activePeers.size(),
        (int) (maxActiveNodes * activeFactor - activePeersCount.get()));
    int lackSize = size - connectNodes.size();
    if (lackSize > 0) {
      //把HomeNode节点加入到正在使用的节点集合中
      nodesInUse.add(nodeManager.getPublicHomeNode().getHexId());

      //根据分数排序 获取前N个node
      List<NodeHandler> newNodes = nodeManager.getNodes(new NodeSelector(nodesInUse), lackSize);
      connectNodes.addAll(newNodes);
    }

    connectNodes.forEach(n -> {
      //主动建立连接
      peerClient.connectAsync(n, false);
      //主动建立的链接加入缓存 180秒不会重复发起TCP连接，避免发起无效的连接请求
      nodeHandlerCache.put(n, System.currentTimeMillis());
    });
  }

  synchronized void logActivePeers() {
    String str = String.format("\n\n============ Peer stats: all %d, active %d, passive %d\n\n",
        channelManager.getActivePeers().size(), activePeersCount.get(), passivePeersCount.get());
    metric(channelManager.getActivePeers().size(), MetricLabels.Gauge.PEERS_ALL);
    metric(activePeersCount.get(), MetricLabels.Gauge.PEERS_ACTIVE);
    metric(passivePeersCount.get(), MetricLabels.Gauge.PEERS_PASSIVE);
    StringBuilder sb = new StringBuilder(str);
    int valid = 0;
    for (PeerConnection peer : new ArrayList<>(activePeers)) {
      sb.append(peer.log());
      appendPeerLatencyLog(sb, peer);
      sb.append("\n");
      if (!(peer.isNeedSyncFromUs() || peer.isNeedSyncFromPeer())) {
        valid++;
      }
    }
    metric(valid, MetricLabels.Gauge.PEERS_VALID);
    logger.info(sb.toString());
  }

  private void metric(double amt, String peerType) {
    Metrics.gaugeSet(MetricKeys.Gauge.PEERS, amt, peerType);
  }

  private void appendPeerLatencyLog(StringBuilder builder, PeerConnection peer) {
    Snapshot peerSnapshot = MetricsUtil.getHistogram(MetricsKey.NET_LATENCY_FETCH_BLOCK
        + peer.getNode().getHost()).getSnapshot();
    builder.append(String.format(
        "top99 : %f, top95 : %f, top75 : %f, max : %d, min : %d, mean : %f, median : %f",
        peerSnapshot.get99thPercentile(), peerSnapshot.get95thPercentile(),
        peerSnapshot.get75thPercentile(), peerSnapshot.getMax(), peerSnapshot.getMin(),
        peerSnapshot.getMean(), peerSnapshot.getMedian())).append("\n");
  }

  public List<PeerConnection> getActivePeers() {
    List<PeerConnection> peers = Lists.newArrayList();
    for (PeerConnection peer : new ArrayList<>(activePeers)) {
      if (!peer.isDisconnect()) {
        peers.add(peer);
      }
    }
    return peers;
  }

  /**
   * 握手成功逻辑
   * 加锁原因是因为需要更新 passivePeersCount  activePeersCount activePeers
   *
   */
  public synchronized void onConnect(Channel peer) {
    PeerConnection peerConnection = (PeerConnection) peer;
    if (!activePeers.contains(peerConnection)) {

      /**
       * 被动建立连接时 默认remoteId为空    PeerClient 主动建立连接时 remoteId为对方的nodeid
       * remoteId不为空时，activePeersCount ++；否则 passivePeersCount ++
       */
      if (!peerConnection.isActive()) {
        passivePeersCount.incrementAndGet();
      } else {
        activePeersCount.incrementAndGet();
      }
      activePeers.add(peerConnection);
      //activePeers 排序  用于FETCH_ENV_DATA 时直接获取延迟最低的节点
      activePeers
          .sort(Comparator.comparingDouble(
              c -> c.getNodeStatistics().pingMessageLatency.getAvg()));
      peerConnection.onConnect();
    }
  }

  public synchronized void onDisconnect(Channel peer) {
    PeerConnection peerConnection = (PeerConnection) peer;
    if (activePeers.contains(peerConnection)) {
      if (!peerConnection.isActive()) {
        passivePeersCount.decrementAndGet();
      } else {
        activePeersCount.decrementAndGet();
      }
      activePeers.remove(peerConnection);
      peerConnection.onDisconnect();
    }
  }

  public boolean isCanConnect() {
    return passivePeersCount.get() < maxActiveNodes * (1 - activeFactor);
  }

  public void close() {
    try {
      poolLoopExecutor.shutdownNow();
      logExecutor.shutdownNow();
    } catch (Exception e) {
      logger.warn("Problems shutting down executor", e);
    }
  }

  public AtomicInteger getPassivePeersCount() {
    return passivePeersCount;
  }

  public AtomicInteger getActivePeersCount() {
    return activePeersCount;
  }

  class NodeSelector implements Predicate<NodeHandler> {

    private Set<String> nodesInUse;

    public NodeSelector(Set<String> nodesInUse) {
      this.nodesInUse = nodesInUse;
    }

    @Override
    //过滤homenode节点 RecentlyDisconnect节点   BadPeers节点 相同ip连接超过2个的节点  正在使用的节点  最近连接的节点 最近握手成功的节点
    public boolean test(NodeHandler handler) {
      long headNum = chainBaseManager.getHeadBlockNum();
      InetAddress inetAddress = handler.getInetSocketAddress().getAddress();
      Protocol.HelloMessage message = channelManager.getHelloMessageCache()
              .getIfPresent(inetAddress.getHostAddress());
      return !((handler.getNode().getHost().equals(nodeManager.getPublicHomeNode().getHost())
              && handler.getNode().getPort() == nodeManager.getPublicHomeNode().getPort())
          || (channelManager.getRecentlyDisconnected().getIfPresent(inetAddress) != null)
          || (channelManager.getBadPeers().getIfPresent(inetAddress) != null)
          || (channelManager.getConnectionNum(inetAddress) >= maxActivePeersWithSameIp)
          || (nodesInUse.contains(handler.getNode().getHexId()))
          || (nodeHandlerCache.getIfPresent(handler) != null)
          || (message != null && headNum < message.getLowestBlockNum()));
    }
  }

}
