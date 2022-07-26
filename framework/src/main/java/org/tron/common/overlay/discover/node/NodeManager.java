package org.tron.common.overlay.discover.node;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.common.net.udp.handler.EventHandler;
import org.tron.common.net.udp.handler.UdpEvent;
import org.tron.common.net.udp.message.Message;
import org.tron.common.net.udp.message.discover.DiscoverMessageInspector;
import org.tron.common.net.udp.message.discover.FindNodeMessage;
import org.tron.common.net.udp.message.discover.NeighborsMessage;
import org.tron.common.net.udp.message.discover.PingMessage;
import org.tron.common.net.udp.message.discover.PongMessage;
import org.tron.common.overlay.discover.node.NodeHandler.State;
import org.tron.common.overlay.discover.node.statistics.NodeStatistics;
import org.tron.common.overlay.discover.table.NodeTable;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.prometheus.MetricKeys;
import org.tron.common.prometheus.MetricLabels;
import org.tron.common.prometheus.Metrics;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.CollectionUtils;
import org.tron.common.utils.JsonUtil;
import org.tron.core.ChainBaseManager;
import org.tron.core.capsule.BytesCapsule;
import org.tron.core.config.args.Args;
import org.tron.core.metrics.MetricsKey;
import org.tron.core.metrics.MetricsUtil;

@Slf4j(topic = "discover")
@Component
public class NodeManager implements EventHandler {

  private static final byte[] DB_KEY_PEERS = "peers".getBytes();
  private static final long DB_COMMIT_RATE = 1 * 60 * 1000L;
  private static final int MAX_NODES = 2000;
  private static final int MAX_NODES_WRITE_TO_DB = 30;
  private static final int NODES_TRIM_THRESHOLD = 3000;
  private CommonParameter commonParameter = Args.getInstance();
  private ChainBaseManager chainBaseManager;
  private Consumer<UdpEvent> messageSender;

  private NodeTable table;
  private Node homeNode;
  private Map<String, NodeHandler> nodeHandlerMap = new ConcurrentHashMap<>();
  private List<Node> bootNodes = new ArrayList<>();

  private volatile boolean discoveryEnabled;

  private volatile boolean inited = false;

  private Timer nodeManagerTasksTimer = new Timer("NodeManagerTasks");

  private ScheduledExecutorService pongTimer;

  @Autowired
  public NodeManager(ChainBaseManager chainBaseManager) {
    this.chainBaseManager = chainBaseManager;
    discoveryEnabled = commonParameter.isNodeDiscoveryEnable();

    /**
     * 设置homeNode 信息
     * homenode 的ip 为ping 得到的真实外网ip
     */
    homeNode = new Node(Node.getNodeId(), commonParameter.getNodeExternalIp(),
        commonParameter.getNodeListenPort());

    //添加配置文件种子节点信息
    for (String boot : commonParameter.getSeedNode().getIpList()) {
      bootNodes.add(Node.instanceOf(boot));
    }

    logger.info("homeNode : {}", homeNode);

    //初始化k表
    table = new NodeTable(homeNode);

    this.pongTimer = Executors.newSingleThreadScheduledExecutor();
  }

  public ScheduledExecutorService getPongTimer() {
    return pongTimer;
  }

  @Override
  public void channelActivated() {
    if (!inited) {
      inited = true;

      //根据配置 读取数据库节点    node节点定时入库
      if (commonParameter.isNodeDiscoveryPersist()) {
        //读取数据库节点 开始节点发现
        dbRead();
        //节点入库
        nodeManagerTasksTimer.scheduleAtFixedRate(new TimerTask() {
          @Override
          public void run() {
            dbWrite();
          }
        }, DB_COMMIT_RATE, DB_COMMIT_RATE);
      }

      //boot 节点开始节点发现
      for (Node node : bootNodes) {
        getNodeHandler(node);
      }
    }
  }

  public boolean isNodeAlive(NodeHandler nodeHandler) {
    return nodeHandler.getState().equals(State.ALIVE)
        || nodeHandler.getState().equals(State.ACTIVE)
        || nodeHandler.getState().equals(State.EVICTCANDIDATE);
  }

  private void dbRead() {
    try {
      byte[] nodeBytes = chainBaseManager.getCommonStore().get(DB_KEY_PEERS).getData();
      if (ByteArray.isEmpty(nodeBytes)) {
        return;
      }
      DBNode dbNode = JsonUtil.json2Obj(new String(nodeBytes), DBNode.class);
      logger.info("Reading node statistics from store: {} nodes.", dbNode.getNodes().size());
      dbNode.getNodes().forEach(n -> {
        Node node = new Node(n.getId(), n.getHost(), n.getPort());
        getNodeHandler(node).getNodeStatistics().setPersistedReputation(n.getReputation());
      });
    } catch (Exception e) {
      logger.error("DB read node failed.", e);
    }
  }

  private void dbWrite() {
    try {
      List<DBNodeStats> batch = new ArrayList<>();
      DBNode dbNode = new DBNode();
      for (NodeHandler nodeHandler : nodeHandlerMap.values()) {
        Node node = nodeHandler.getNode();
        if (node.isConnectible(Args.getInstance().getNodeP2pVersion())) {
          DBNodeStats nodeStatic = new DBNodeStats(node.getId(), node.getHost(),
              node.getPort(), nodeHandler.getNodeStatistics().getReputation());
          batch.add(nodeStatic);
        }
      }
      int size = batch.size();
      batch.sort(Comparator.comparingInt(value -> -value.getReputation()));
      if (batch.size() > MAX_NODES_WRITE_TO_DB) {
        batch = batch.subList(0, MAX_NODES_WRITE_TO_DB);
      }

      dbNode.setNodes(batch);

      logger.info("Write node statistics to store: m:{}/t:{}/{}/{} nodes.",
          nodeHandlerMap.size(), getTable().getAllNodes().size(), size, batch.size());

      chainBaseManager.getCommonStore()
          .put(DB_KEY_PEERS, new BytesCapsule(JsonUtil.obj2Json(dbNode).getBytes()));
    } catch (Exception e) {
      logger.error("DB write node failed.", e);
    }
  }

  public void setMessageSender(Consumer<UdpEvent> messageSender) {
    this.messageSender = messageSender;
  }

  private String getKey(Node n) {
    return getKey(new InetSocketAddress(n.getHost(), n.getPort()));
  }

  private String getKey(InetSocketAddress address) {
    InetAddress inetAddress = address.getAddress();
    return (inetAddress == null ? address.getHostString() : inetAddress.getHostAddress()) + ":"
        + address.getPort();
  }

  //新节点放入nodeHandlerMap 中默认状态为 DISCOVERED
  public NodeHandler getNodeHandler(Node n) {
    String key = getKey(n);
    NodeHandler ret = nodeHandlerMap.get(key);
    if (ret == null) {
      //整理nodeHandlerMap
      trimTable();
      //新的node 默认置为 DISCOVERED 状态
      ret = new NodeHandler(n, this);
      nodeHandlerMap.put(key, ret);
    } else if (ret.getNode().isDiscoveryNode() && !n.isDiscoveryNode()) {
      // nodeHandlerMap中节点的信息是FakeNodeId  新更新的节点信息不是FakeNodeId  则更新 真实的 node节点 信息
      ret.setNode(n);
    }
    return ret;
  }

  //整理nodeHandlerMap
  private void trimTable() {
    //nodeHandlerMap最多有3000个节点
    if (nodeHandlerMap.size() > NODES_TRIM_THRESHOLD) {
      //去除p2p版本不一致的节点
      nodeHandlerMap.values().forEach(handler -> {
        if (!handler.getNode().isConnectible(Args.getInstance().getNodeP2pVersion())) {
          nodeHandlerMap.values().remove(handler);
        }
      });
    }
    if (nodeHandlerMap.size() > NODES_TRIM_THRESHOLD) {
      //根据分数排序 整理 nodeHandlerMap 到2000
      List<NodeHandler> sorted = new ArrayList<>(nodeHandlerMap.values());
      sorted.sort(Comparator.comparingInt(o -> o.getNodeStatistics().getReputation()));
      for (NodeHandler handler : sorted) {
        nodeHandlerMap.values().remove(handler);
        if (nodeHandlerMap.size() <= MAX_NODES) {
          break;
        }
      }
    }
  }

  public boolean hasNodeHandler(Node n) {
    return nodeHandlerMap.containsKey(getKey(n));
  }

  public NodeTable getTable() {
    return table;
  }

  public NodeStatistics getNodeStatistics(Node n) {
    return getNodeHandler(n).getNodeStatistics();
  }

  /**
   * 接收到节点发现消息后 重置 node 的真实id   isFakeNodeId 置为false
   */
  @Override
  public void handleEvent(UdpEvent udpEvent) {
    Message m = udpEvent.getMessage();
    if (!DiscoverMessageInspector.valid(m)) {
      return;
    }

    InetSocketAddress sender = udpEvent.getAddress();

    Node n = new Node(m.getFrom().getId(), sender.getHostString(), sender.getPort(),
        m.getFrom().getPort());

    NodeHandler nodeHandler = getNodeHandler(n);
    nodeHandler.getNodeStatistics().messageStatistics.addUdpInMessage(m.getType());
    int length = udpEvent.getMessage().getData().length + 1;
    MetricsUtil.meterMark(MetricsKey.NET_UDP_IN_TRAFFIC, length);
    Metrics.histogramObserve(MetricKeys.Histogram.UDP_BYTES, length,
        MetricLabels.Histogram.TRAFFIC_IN);

    switch (m.getType()) {
      case DISCOVER_PING:
        nodeHandler.handlePing((PingMessage) m);
        break;
      case DISCOVER_PONG:
        nodeHandler.handlePong((PongMessage) m);
        break;
      case DISCOVER_FIND_NODE:
        nodeHandler.handleFindNode((FindNodeMessage) m);
        break;
      case DISCOVER_NEIGHBORS:
        nodeHandler.handleNeighbours((NeighborsMessage) m);
        break;
      default:
        break;
    }
  }

  public void sendOutbound(UdpEvent udpEvent) {
    if (discoveryEnabled && messageSender != null) {
      messageSender.accept(udpEvent);
      int length = udpEvent.getMessage().getSendData().length;
      MetricsUtil.meterMark(MetricsKey.NET_UDP_OUT_TRAFFIC, length);
      Metrics.histogramObserve(MetricKeys.Histogram.UDP_BYTES, length,
          MetricLabels.Histogram.TRAFFIC_OUT);

    }
  }

  public List<NodeHandler> getNodes(Predicate<NodeHandler> predicate, int limit) {
    List<NodeHandler> filtered = new ArrayList<>();
    for (NodeHandler handler : nodeHandlerMap.values()) {
      //所有节点中过滤 排除条件 过滤p2p版本 端口
      if (handler.getNode().isConnectible(Args.getInstance().getNodeP2pVersion())
          //过滤homenode节点 RecentlyDisconnect节点   BadPeers节点 相同ip连接超过2个的节点  正在使用的节点  最近连接的节点
          && predicate.test(handler)) {
        //设置分数
        handler.setReputation(handler.getNodeStatistics().getReputation());
        filtered.add(handler);
      }
    }
    //根据分数倒序排列  选取前limit个node 返回
    filtered.sort(Comparator.comparingInt(handler -> -handler.getReputation()));
    return CollectionUtils.truncate(filtered, limit);
  }

  public List<NodeHandler> dumpActiveNodes() {
    List<NodeHandler> handlers = new ArrayList<>();
    for (NodeHandler handler : this.nodeHandlerMap.values()) {
      if (isNodeAlive(handler)) {
        handlers.add(handler);
      }
    }
    return handlers;
  }

  public Node getPublicHomeNode() {
    return homeNode;
  }

  // just for test
  public void clearNodeHandlerMap() {
    nodeHandlerMap.clear();
  }

  public void close() {
    try {
      nodeManagerTasksTimer.cancel();
      pongTimer.shutdownNow();
    } catch (Exception e) {
      logger.warn("close failed.", e);
    }
  }

}
