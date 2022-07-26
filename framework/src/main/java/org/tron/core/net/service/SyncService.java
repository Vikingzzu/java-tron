package org.tron.core.net.service;

import static org.tron.core.config.Parameter.NetConstants.MAX_BLOCK_FETCH_PER_PEER;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.common.overlay.server.Channel.TronState;
import org.tron.common.utils.Pair;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.BlockCapsule.BlockId;
import org.tron.core.config.Parameter.NetConstants;
import org.tron.core.exception.P2pException;
import org.tron.core.exception.P2pException.TypeEnum;
import org.tron.core.net.TronNetDelegate;
import org.tron.core.net.message.BlockMessage;
import org.tron.core.net.message.FetchInvDataMessage;
import org.tron.core.net.message.SyncBlockChainMessage;
import org.tron.core.net.messagehandler.PbftDataSyncHandler;
import org.tron.core.net.peer.PeerConnection;
import org.tron.protos.Protocol.Inventory.InventoryType;
import org.tron.protos.Protocol.ReasonCode;

@Slf4j(topic = "net")
@Component
public class SyncService {

  @Autowired
  private TronNetDelegate tronNetDelegate;

  @Autowired
  private PbftDataSyncHandler pbftDataSyncHandler;

  //收到BLOCK消息异步处理集合
  private Map<BlockMessage, PeerConnection> blockWaitToProcess = new ConcurrentHashMap<>();

  //收到BLOCK消息
  private Map<BlockMessage, PeerConnection> blockJustReceived = new ConcurrentHashMap<>();

  //发送FEV_INV_DATA的block缓存结合
  private Cache<BlockId, Long> requestBlockIds = CacheBuilder.newBuilder().maximumSize(10_000)
      .expireAfterWrite(1, TimeUnit.HOURS).initialCapacity(10_000)
      .recordStats().build();

  private ScheduledExecutorService fetchExecutor = Executors.newSingleThreadScheduledExecutor();

  private ScheduledExecutorService blockHandleExecutor = Executors
      .newSingleThreadScheduledExecutor();

  /**
   * 出发场景是收到FETCH_ENV_DATA返回的Block消息时触发
   */
  private volatile boolean handleFlag = false;

  /**
   * 有三个触发逻辑
   * 1.收到ChainInventoryMsgHandler消息后，把要同步的块放入SyncBlockToFetch队列大于2000时触发
   * 2.发送FETCH_ENV_DATA 返回的Block消息收到后，判断peer.isIdle 切不需要在fetch区块时 触发
   */
  @Setter
  private volatile boolean fetchFlag = false;

  public void init() {
    //定期同步区块 发送fetch_env_data message
    fetchExecutor.scheduleWithFixedDelay(() -> {
      try {
        if (fetchFlag) {
          fetchFlag = false;
          startFetchSyncBlock();
        }
      } catch (Exception e) {
        logger.error("Fetch sync block error.", e);
      }
    }, 10, 1, TimeUnit.SECONDS);

    //定期处理同步区块
    blockHandleExecutor.scheduleWithFixedDelay(() -> {
      try {
        if (handleFlag) {
          handleFlag = false;
          handleSyncBlock();
        }
      } catch (Exception e) {
        logger.error("Handle sync block error.", e);
      }
    }, 10, 1, TimeUnit.SECONDS);
  }

  public void close() {
    fetchExecutor.shutdown();
    blockHandleExecutor.shutdown();
  }

  public void startSync(PeerConnection peer) {
    peer.setTronState(TronState.SYNCING);
    peer.setNeedSyncFromPeer(true);
    peer.getSyncBlockToFetch().clear();
    peer.setRemainNum(0);
    peer.setBlockBothHave(tronNetDelegate.getGenesisBlockId());
    syncNext(peer);
  }

  //开始同步区块
  public void syncNext(PeerConnection peer) {
    try {
      if (peer.getSyncChainRequested() != null) {
        logger.warn("Peer {} is in sync.", peer.getNode().getHost());
        return;
      }
      LinkedList<BlockId> chainSummary = getBlockChainSummary(peer);
      peer.setSyncChainRequested(new Pair<>(chainSummary, System.currentTimeMillis()));
      peer.sendMessage(new SyncBlockChainMessage(chainSummary));
    } catch (Exception e) {
      logger.error("Peer {} sync failed, reason: {}", peer.getInetAddress(), e.getMessage());
      peer.disconnect(ReasonCode.SYNC_FAIL);
    }
  }

  //主动发送fetch_env_data获取的block处理逻辑
  public void processBlock(PeerConnection peer, BlockMessage blockMessage) {
    //加锁的原因是 另一个线程handleSyncBlock逻辑 会把blockJustReceived 放置到 blockWaitToProcess中，并清空blockJustReceived
    synchronized (blockJustReceived) {
      blockJustReceived.put(blockMessage, peer);
    }
    handleFlag = true;
    if (peer.isIdle()) {
      if (peer.getRemainNum() > 0
          && peer.getSyncBlockToFetch().size() <= NetConstants.SYNC_FETCH_BATCH_NUM) {
        syncNext(peer);
      } else {
        fetchFlag = true;
      }
    }
  }

  public void onDisconnect(PeerConnection peer) {
    if (!peer.getSyncBlockRequested().isEmpty()) {
      peer.getSyncBlockRequested().keySet().forEach(blockId -> invalid(blockId));
    }
  }

  private void invalid(BlockId blockId) {
    requestBlockIds.invalidate(blockId);
    fetchFlag = true;
  }

  private LinkedList<BlockId> getBlockChainSummary(PeerConnection peer) throws P2pException {

    BlockId beginBlockId = peer.getBlockBothHave();
    List<BlockId> blockIds = new ArrayList<>(peer.getSyncBlockToFetch());
    List<BlockId> forkList = new LinkedList<>();
    LinkedList<BlockId> summary = new LinkedList<>();
    long syncBeginNumber = tronNetDelegate.getSyncBeginNumber();
    long low = syncBeginNumber < 0 ? 0 : syncBeginNumber;
    long highNoFork;
    long high;

    if (beginBlockId.getNum() == 0) {
      highNoFork = high = tronNetDelegate.getHeadBlockId().getNum();
    } else {
      if (tronNetDelegate.containBlockInMainChain(beginBlockId)) {
        highNoFork = high = beginBlockId.getNum();
      } else {
        forkList = tronNetDelegate.getBlockChainHashesOnFork(beginBlockId);
        if (forkList.isEmpty()) {
          throw new P2pException(TypeEnum.SYNC_FAILED,
              "can't find blockId: " + beginBlockId.getString());
        }
        highNoFork = ((LinkedList<BlockId>) forkList).peekLast().getNum();
        ((LinkedList) forkList).pollLast();
        Collections.reverse(forkList);
        high = highNoFork + forkList.size();
      }
    }

    if (low > highNoFork) {
      throw new P2pException(TypeEnum.SYNC_FAILED, "low: " + low + " gt highNoFork: " + highNoFork);
    }

    long realHigh = high + blockIds.size();

    logger.info("Get block chain summary, low: {}, highNoFork: {}, high: {}, realHigh: {}",
        low, highNoFork, high, realHigh);

    while (low <= realHigh) {
      if (low <= highNoFork) {
        summary.offer(tronNetDelegate.getBlockIdByNum(low));
      } else if (low <= high) {
        summary.offer(forkList.get((int) (low - highNoFork - 1)));
      } else {
        summary.offer(blockIds.get((int) (low - high - 1)));
      }
      low += (realHigh - low + 2) / 2;
    }

    return summary;
  }

  private void startFetchSyncBlock() {
    HashMap<PeerConnection, List<BlockId>> send = new HashMap<>();

    tronNetDelegate.getActivePeer().stream()
        .filter(peer -> peer.isNeedSyncFromPeer() && peer.isIdle())
        .forEach(peer -> {
          if (!send.containsKey(peer)) {
            send.put(peer, new LinkedList<>());
          }
          for (BlockId blockId : peer.getSyncBlockToFetch()) {
            if (requestBlockIds.getIfPresent(blockId) == null) {
              requestBlockIds.put(blockId, System.currentTimeMillis());
              peer.getSyncBlockRequested().put(blockId, System.currentTimeMillis());
              send.get(peer).add(blockId);
              //每个peer 每次最多获取100个block
              if (send.get(peer).size() >= MAX_BLOCK_FETCH_PER_PEER) {
                break;
              }
            }
          }
        });

    send.forEach((peer, blockIds) -> {
      if (!blockIds.isEmpty()) {
        peer.sendMessage(new FetchInvDataMessage(new LinkedList<>(blockIds), InventoryType.BLOCK));
      }
    });
  }

  //处理发送FETCH_ENV_DATA消息返回的Block消息的逻辑
  private synchronized void handleSyncBlock() {

    //加锁的原因是其他线程会同时往blockJustReceived中塞数据
    synchronized (blockJustReceived) {
      blockWaitToProcess.putAll(blockJustReceived);
      blockJustReceived.clear();
    }

    final boolean[] isProcessed = {true};

    while (isProcessed[0]) {

      isProcessed[0] = false;
      //加锁的目的是为了防止其他线程同时改变 SyncBlockToFetch 和 blockWaitToProcess
      synchronized (tronNetDelegate.getBlockLock()) {
        blockWaitToProcess.forEach((msg, peerConnection) -> {
          //在区块入库之前检查peer是否断开，如果断开 有可能这个节点时坏节点   抛弃它发给我的区块
          if (peerConnection.isDisconnect()) {
            blockWaitToProcess.remove(msg);
            invalid(msg.getBlockId());
            return;
          }
          final boolean[] isFound = {false};
          //找到所有peer的SyncBlockToFetch中已接收的block  并删除
          tronNetDelegate.getActivePeer().stream()
              .filter(peer -> msg.getBlockId().equals(peer.getSyncBlockToFetch().peek()))
              .forEach(peer -> {
                peer.getSyncBlockToFetch().pop();
                peer.getSyncBlockInProcess().add(msg.getBlockId());
                isFound[0] = true;
              });
          if (isFound[0]) {
            blockWaitToProcess.remove(msg);
            isProcessed[0] = true;
            //同步区块入库
            processSyncBlock(msg.getBlockCapsule());
          }
        });
      }
    }
  }

  //同步区块入库
  private void processSyncBlock(BlockCapsule block) {
    boolean flag = true;
    BlockId blockId = block.getBlockId();
    try {
      tronNetDelegate.validSignature(block);
      tronNetDelegate.processBlock(block, true);
      pbftDataSyncHandler.processPBFTCommitData(block);
    } catch (Exception e) {
      logger.error("Process sync block {} failed.", blockId.getString(), e);
      flag = false;
    }
    for (PeerConnection peer : tronNetDelegate.getActivePeer()) {
      if (peer.getSyncBlockInProcess().remove(blockId)) {
        if (flag) {
          peer.setBlockBothHave(blockId);
          if (peer.getSyncBlockToFetch().isEmpty()) {
            syncNext(peer);
          }
        } else {
          peer.disconnect(ReasonCode.BAD_BLOCK);
        }
      }
    }
  }

}
