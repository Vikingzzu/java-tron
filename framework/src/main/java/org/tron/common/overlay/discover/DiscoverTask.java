package org.tron.common.overlay.discover;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.tron.common.overlay.discover.node.Node;
import org.tron.common.overlay.discover.node.NodeManager;
import org.tron.common.overlay.discover.table.KademliaOptions;

@Slf4j(topic = "discover")
public class DiscoverTask implements Runnable {

  private NodeManager nodeManager;

  private byte[] nodeId;

  public DiscoverTask(NodeManager nodeManager) {
    this.nodeManager = nodeManager;
    this.nodeId = nodeManager.getPublicHomeNode().getId();
  }

  @Override
  public void run() {
    discover(nodeId, 0, new ArrayList<>());
  }

  public synchronized void discover(byte[] nodeId, int round, List<Node> prevTried) {

    try {
      //每轮discover最多执行8次    意味着每轮最多寻找24个节点
      if (round == KademliaOptions.MAX_STEPS) {
        return;
      }

      //找出离目标节点最近的16个 真实 node id节点
      List<Node> closest = nodeManager.getTable().getClosestNodes(nodeId);
      List<Node> tried = new ArrayList<>();
      for (Node n : closest) {
        if (!tried.contains(n) && !prevTried.contains(n)) {
          try {
            //寻找邻居节点
            nodeManager.getNodeHandler(n).sendFindNode(nodeId);
            tried.add(n);
            wait(50);
          } catch (Exception ex) {
            logger.error("Unexpected Exception " + ex, ex);
          }
        }
        //每次最多寻找3个节点    意味着每轮最多寻找24个节点
        if (tried.size() == KademliaOptions.ALPHA) {
          break;
        }
      }

      if (tried.isEmpty()) {
        return;
      }

      tried.addAll(prevTried);

      discover(nodeId, round + 1, tried);
    } catch (Exception ex) {
      logger.error("{}", ex);
    }
  }

}
