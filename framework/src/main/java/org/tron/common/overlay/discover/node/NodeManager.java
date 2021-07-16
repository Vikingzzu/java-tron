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
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.CollectionUtils;
import org.tron.common.utils.JsonUtil;
import org.tron.core.ChainBaseManager;
import org.tron.core.capsule.BytesCapsule;
import org.tron.core.config.args.Args;
import org.tron.core.metrics.MetricsKey;
import org.tron.core.metrics.MetricsUtil;

/**
 * p2p连接node管理类
 */
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
  //缓存p2p连接nodeHandler  key：node对应address的地址加端口  value: nodeHandler
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

    homeNode = new Node(Node.getNodeId(), commonParameter.getNodeExternalIp(),
        commonParameter.getNodeListenPort());

    for (String boot : commonParameter.getSeedNode().getIpList()) {
      bootNodes.add(Node.instanceOf(boot));
    }

    logger.info("homeNode : {}", homeNode);

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

      if (commonParameter.isNodeDiscoveryPersist()) {
        dbRead();
        nodeManagerTasksTimer.scheduleAtFixedRate(new TimerTask() {
          @Override
          public void run() {
            dbWrite();
          }
        }, DB_COMMIT_RATE, DB_COMMIT_RATE);
      }

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

  //拼接address地址端口字符串
  private String getKey(InetSocketAddress address) {
    InetAddress inetAddress = address.getAddress();
    return (inetAddress == null ? address.getHostString() : inetAddress.getHostAddress()) + ":"
        + address.getPort();
  }

  //获取node对应的NodeHandler 存在则取出 不存在则创建
  public NodeHandler getNodeHandler(Node n) {
    //拼接address地址端口字符串
    String key = getKey(n);
    NodeHandler ret = nodeHandlerMap.get(key);
    if (ret == null) {
      //nodeHandlerMap缓存缩容逻辑
      trimTable();
      ret = new NodeHandler(n, this);
      //新node 入缓存
      nodeHandlerMap.put(key, ret);
    } else if (ret.getNode().isDiscoveryNode() && !n.isDiscoveryNode()) {
      //map里存的是伪造节点  但node不是伪造节点
      ret.setNode(n);
    }
    return ret;
  }

  //nodeHandlerMap缓存缩容逻辑
  private void trimTable() {
    //判断map的大小大于 3000 则清楚p2p版本不一致的node handler
    if (nodeHandlerMap.size() > NODES_TRIM_THRESHOLD) {
      nodeHandlerMap.values().forEach(handler -> {
        if (!handler.getNode().isConnectible(Args.getInstance().getNodeP2pVersion())) {
          nodeHandlerMap.values().remove(handler);
        }
      });
    }
    ////判断map的大小还是大于 3000  则根据node 的商誉分数排序  清除map直到 2000
    if (nodeHandlerMap.size() > NODES_TRIM_THRESHOLD) {
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

  @Override
  //处理接收到的p2p网络发现msg
  public void handleEvent(UdpEvent udpEvent) {
    Message m = udpEvent.getMessage();
    //校验消息类型
    if (!DiscoverMessageInspector.valid(m)) {
      return;
    }

    InetSocketAddress sender = udpEvent.getAddress();

    Node n = new Node(m.getFrom().getId(), sender.getHostString(), sender.getPort(),
        m.getFrom().getPort());

    NodeHandler nodeHandler = getNodeHandler(n);
    //添加业务统计
    nodeHandler.getNodeStatistics().messageStatistics.addUdpInMessage(m.getType());
    MetricsUtil.meterMark(MetricsKey.NET_UDP_IN_TRAFFIC,
        udpEvent.getMessage().getData().length + 1);

    switch (m.getType()) {
      //节点发现ping
      case DISCOVER_PING:
        nodeHandler.handlePing((PingMessage) m);
        break;
      //节点发现pong
      case DISCOVER_PONG:
        nodeHandler.handlePong((PongMessage) m);
        break;
        //返回当前邻居节点
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
      MetricsUtil.meterMark(MetricsKey.NET_UDP_OUT_TRAFFIC,
          udpEvent.getMessage().getSendData().length);
    }
  }

  public List<NodeHandler> getNodes(Predicate<NodeHandler> predicate, int limit) {
    List<NodeHandler> filtered = new ArrayList<>();
    for (NodeHandler handler : nodeHandlerMap.values()) {
      if (handler.getNode().isConnectible(Args.getInstance().getNodeP2pVersion())
          && predicate.test(handler)) {
        filtered.add(handler);
      }
    }
    filtered.sort(Comparator.comparingInt(handler -> -handler.getNodeStatistics().getReputation()));
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

  public void close() {
    try {
      nodeManagerTasksTimer.cancel();
      pongTimer.shutdownNow();
    } catch (Exception e) {
      logger.warn("close failed.", e);
    }
  }

}
