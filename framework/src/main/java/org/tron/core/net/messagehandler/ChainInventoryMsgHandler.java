package org.tron.core.net.messagehandler;

import static org.tron.core.config.Parameter.ChainConstant.BLOCK_PRODUCED_INTERVAL;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.core.capsule.BlockCapsule.BlockId;
import org.tron.core.config.Parameter.ChainConstant;
import org.tron.core.config.Parameter.NetConstants;
import org.tron.core.exception.P2pException;
import org.tron.core.exception.P2pException.TypeEnum;
import org.tron.core.net.TronNetDelegate;
import org.tron.core.net.message.ChainInventoryMessage;
import org.tron.core.net.message.TronMessage;
import org.tron.core.net.peer.PeerConnection;
import org.tron.core.net.service.SyncService;

@Slf4j(topic = "net")
@Component
public class ChainInventoryMsgHandler implements TronMsgHandler {

  @Autowired
  private TronNetDelegate tronNetDelegate;

  @Autowired
  private SyncService syncService;

  @Override
  public void processMessage(PeerConnection peer, TronMessage msg) throws P2pException {

    ChainInventoryMessage chainInventoryMessage = (ChainInventoryMessage) msg;

    check(peer, chainInventoryMessage);

    peer.setNeedSyncFromPeer(true);

    peer.setSyncChainRequested(null);

    Deque<BlockId> blockIdWeGet = new LinkedList<>(chainInventoryMessage.getBlockIds());

    if (blockIdWeGet.size() == 1 && tronNetDelegate.containBlock(blockIdWeGet.peek())) {
      peer.setNeedSyncFromPeer(false);
      return;
    }

    /**
     * 如果对方的链发生了链分叉  则会导致 peer.getSyncBlockToFetch().peekLast() 和 blockIdWeGet.peekFirst()  不一致的情况
     * 我们依次去掉SyncBlockToFetch队列的末位元素 直至我们的SyncBlockToFetch队列  和 对方发来的 blockIdWeGet列表首位相接
     * 对方链分叉的场景  对方链分叉会导致我们的bothhave 出现在对方的分叉链上， 需要我们同步对方的主链
     */
    while (!peer.getSyncBlockToFetch().isEmpty()) {
      if (peer.getSyncBlockToFetch().peekLast().equals(blockIdWeGet.peekFirst())) {
        break;
      }
      peer.getSyncBlockToFetch().pollLast();
    }

    blockIdWeGet.poll();

    peer.setRemainNum(chainInventoryMessage.getRemainNum());
    peer.getSyncBlockToFetch().addAll(blockIdWeGet);

    /**
     * 加锁的原因是同步区块的线程会同时处理peer.getSyncBlockToFetch()队列，
     * 如果同步区块的线程   没法继续执行
     */
    synchronized (tronNetDelegate.getBlockLock()) {
      while (!peer.getSyncBlockToFetch().isEmpty() && tronNetDelegate
          .containBlock(peer.getSyncBlockToFetch().peek())) {
        try {
          //如果部分块已经同步  则更新BothHave  并删除SyncBlockToFetch队列中的这部分块
          BlockId blockId = peer.getSyncBlockToFetch().pop();
          peer.setBlockBothHave(blockId);
          logger.info("Block {} from {} is processed",
                  blockId.getString(), peer.getNode().getHost());
        } catch (NoSuchElementException e) {
          logger.warn("Process ChainInventoryMessage failed, peer {}, isDisconnect:{}",
                  peer.getNode().getHost(), peer.isDisconnect());
          return;
        }
      }
    }

    //在 SyncBlockToFetch队列 大于2000时开始同步区块   （因为每次最多同步2000个块，所以队列的大小最多是4000）
    if ((chainInventoryMessage.getRemainNum() == 0 && !peer.getSyncBlockToFetch().isEmpty())
        || (chainInventoryMessage.getRemainNum() != 0
        && peer.getSyncBlockToFetch().size() > NetConstants.SYNC_FETCH_BATCH_NUM)) {
      //开始发送FETCH_INV_DATA获取block
      syncService.setFetchFlag(true);
    } else {
      syncService.syncNext(peer);
    }
  }

  private void check(PeerConnection peer, ChainInventoryMessage msg) throws P2pException {
    if (peer.getSyncChainRequested() == null) {
      throw new P2pException(TypeEnum.BAD_MESSAGE, "not send syncBlockChainMsg");
    }

    List<BlockId> blockIds = msg.getBlockIds();
    if (CollectionUtils.isEmpty(blockIds)) {
      throw new P2pException(TypeEnum.BAD_MESSAGE, "blockIds is empty");
    }

    if (blockIds.size() > NetConstants.SYNC_FETCH_BATCH_NUM + 1) {
      throw new P2pException(TypeEnum.BAD_MESSAGE, "big blockIds size: " + blockIds.size());
    }

    if (msg.getRemainNum() != 0 && blockIds.size() < NetConstants.SYNC_FETCH_BATCH_NUM) {
      throw new P2pException(TypeEnum.BAD_MESSAGE,
          "remain: " + msg.getRemainNum() + ", blockIds size: " + blockIds.size());
    }

    long num = blockIds.get(0).getNum();
    for (BlockId id : msg.getBlockIds()) {
      if (id.getNum() != num++) {
        throw new P2pException(TypeEnum.BAD_MESSAGE, "not continuous block");
      }
    }

    if (!peer.getSyncChainRequested().getKey().contains(blockIds.get(0))) {
      throw new P2pException(TypeEnum.BAD_MESSAGE, "unlinked block, my head: "
          + peer.getSyncChainRequested().getKey().getLast().getString()
          + ", peer: " + blockIds.get(0).getString());
    }

    if (tronNetDelegate.getHeadBlockId().getNum() > 0) {
      long maxRemainTime =
          ChainConstant.CLOCK_MAX_DELAY + System.currentTimeMillis() - tronNetDelegate
              .getBlockTime(tronNetDelegate.getSolidBlockId());
      long maxFutureNum =
          maxRemainTime / BLOCK_PRODUCED_INTERVAL + tronNetDelegate.getSolidBlockId().getNum();
      long lastNum = blockIds.get(blockIds.size() - 1).getNum();
      if (lastNum + msg.getRemainNum() > maxFutureNum) {
        throw new P2pException(TypeEnum.BAD_MESSAGE, "lastNum: " + lastNum + " + remainNum: "
            + msg.getRemainNum() + " > futureMaxNum: " + maxFutureNum);
      }
    }
  }

}
