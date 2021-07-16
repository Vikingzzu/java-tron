package org.tron.core.net.messagehandler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.config.args.Args;
import org.tron.core.net.TronNetDelegate;
import org.tron.core.net.message.InventoryMessage;
import org.tron.core.net.message.TronMessage;
import org.tron.core.net.peer.Item;
import org.tron.core.net.peer.PeerConnection;
import org.tron.core.net.service.AdvService;
import org.tron.protos.Protocol.Inventory.InventoryType;

/**
 * 清单消息handler
 */
@Slf4j(topic = "net")
@Component
public class InventoryMsgHandler implements TronMsgHandler {

  @Autowired
  private TronNetDelegate tronNetDelegate;

  @Autowired
  private AdvService advService;

  @Autowired
  private TransactionsMsgHandler transactionsMsgHandler;

  private int maxCountIn10s = 10_000;

  @Override
  public void processMessage(PeerConnection peer, TronMessage msg) {
    InventoryMessage inventoryMessage = (InventoryMessage) msg;
    InventoryType type = inventoryMessage.getInventoryType();

    //校验消息
    if (!check(peer, inventoryMessage)) {
      return;
    }

    for (Sha256Hash id : inventoryMessage.getHashList()) {
      //封装item
      Item item = new Item(id, type);
      //advInvReceive 缓存 清单item
      peer.getAdvInvReceive().put(item, System.currentTimeMillis());

      //处理接收清单消息的item
      advService.addInv(item);
    }
  }

  //校验消息
  private boolean check(PeerConnection peer, InventoryMessage inventoryMessage) {
    InventoryType type = inventoryMessage.getInventoryType();
    int size = inventoryMessage.getHashList().size();

    //peer间需要同步区块的过滤掉   找出不需要同步区块的连接
    if (peer.isNeedSyncFromPeer() || peer.isNeedSyncFromUs()) {
      logger.warn("Drop inv: {} size: {} from Peer {}, syncFromUs: {}, syncFromPeer: {}.",
          type, size, peer.getInetAddress(), peer.isNeedSyncFromUs(), peer.isNeedSyncFromPeer());
      return false;
    }

    //交易消息
    if (type.equals(InventoryType.TRX)) {
      int count = peer.getNodeStatistics().messageStatistics.tronInTrxInventoryElement.getCount(10);
      //节点过去10s接收到的交易数大于10_000 则校验不通过 防攻击
      if (count > maxCountIn10s) {
        logger.warn("Drop inv: {} size: {} from Peer {}, Inv count: {} is overload.",
            type, size, peer.getInetAddress(), count);
        if (Args.getInstance().isOpenPrintLog()) {
          logger.warn("[overload]Drop tx list is: {}", inventoryMessage.getHashList());
        }
        return false;
      }

      //判断交易handler是否繁忙
      if (transactionsMsgHandler.isBusy()) {
        logger.warn("Drop inv: {} size: {} from Peer {}, transactionsMsgHandler is busy.",
            type, size, peer.getInetAddress());
        if (Args.getInstance().isOpenPrintLog()) {
          logger.warn("[isBusy]Drop tx list is: {}", inventoryMessage.getHashList());
        }
        return false;
      }
    }

    return true;
  }
}
