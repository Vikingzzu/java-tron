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

/**
 * 异步线程同步服务
 * 启动两个单例的异步线程
 */
@Slf4j(topic = "net")
@Component
public class SyncService {

  @Autowired
  private TronNetDelegate tronNetDelegate;

  @Autowired
  private PbftDataSyncHandler pbftDataSyncHandler;

  private Map<BlockMessage, PeerConnection> blockWaitToProcess = new ConcurrentHashMap<>();

  private Map<BlockMessage, PeerConnection> blockJustReceived = new ConcurrentHashMap<>();

  //缓存最近同步区块blockId 1小时后过期
  private Cache<BlockId, Long> requestBlockIds = CacheBuilder.newBuilder().maximumSize(10_000)
      .expireAfterWrite(1, TimeUnit.HOURS).initialCapacity(10_000)
      .recordStats().build();

  private ScheduledExecutorService fetchExecutor = Executors.newSingleThreadScheduledExecutor();

  private ScheduledExecutorService blockHandleExecutor = Executors
      .newSingleThreadScheduledExecutor();

  private volatile boolean handleFlag = false;

  @Setter
  //是否可以开始同步区块具体数据标识
  private volatile boolean fetchFlag = false;

  //初始化
  public void init() {
    //启动同步区块具体数据的异步线程（从syncBlockToFetch中拿到blocks 发送FetchInvDataMessage消息）
    fetchExecutor.scheduleWithFixedDelay(() -> {
      try {
        if (fetchFlag) {
          fetchFlag = false;
          //开始同步区块具体数据
          startFetchSyncBlock();
        }
      } catch (Exception e) {
        logger.error("Fetch sync block error.", e);
      }
    }, 10, 1, TimeUnit.SECONDS);

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

  //发起区块儿同步
  public void startSync(PeerConnection peer) {
    peer.setTronState(TronState.SYNCING);
    peer.setNeedSyncFromPeer(true);
    //清空涉及到区块儿同步的值
    peer.getSyncBlockToFetch().clear();
    peer.setRemainNum(0);
    peer.setBlockBothHave(tronNetDelegate.getGenesisBlockId());
    //发起同步区块儿方法
    syncNext(peer);
  }

  //发起开始同步区块儿方法 发送SyncBlockChainMessage消息 带上自己的摘要信息
  public void syncNext(PeerConnection peer) {
    try {
      //判断当前peer是否已经发送过请求区块的行为
      if (peer.getSyncChainRequested() != null) {
        logger.warn("Peer {} is in sync.", peer.getNode().getHost());
        return;
      }
      //计算本节点区块儿摘要
      LinkedList<BlockId> chainSummary = getBlockChainSummary(peer);
      //设置同步摘要内容
      peer.setSyncChainRequested(new Pair<>(chainSummary, System.currentTimeMillis()));
      //发送SyncBlockChainMessage消息（带上chainSummary）
      peer.sendMessage(new SyncBlockChainMessage(chainSummary));
    } catch (Exception e) {
      logger.error("Peer {} sync failed, reason: {}", peer.getInetAddress(), e.getMessage());
      peer.disconnect(ReasonCode.SYNC_FAIL);
    }
  }

  public void processBlock(PeerConnection peer, BlockMessage blockMessage) {
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

  //计算区块儿同步摘要 TODO 逻辑暂时搁置
  private LinkedList<BlockId> getBlockChainSummary(PeerConnection peer) throws P2pException {
    //起始的区块儿
    BlockId beginBlockId = peer.getBlockBothHave();
    //syncBlockToFetch 的 block 也算是本节点的 block
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

  //同步区块具体数据方法
  private void startFetchSyncBlock() {
    //储存发送消息map key:Connection  value:blockIds
    HashMap<PeerConnection, List<BlockId>> send = new HashMap<>();

    //拿到所有的需要同步区块的peer连接
    tronNetDelegate.getActivePeer().stream()
            //需要从peer同步区块 且 TODO
        .filter(peer -> peer.isNeedSyncFromPeer() && peer.isIdle())
        .forEach(peer -> {
          //缓存Connection
          if (!send.containsKey(peer)) {
            send.put(peer, new LinkedList<>());
          }
          //遍历syncBlockToFetch集合
          for (BlockId blockId : peer.getSyncBlockToFetch()) {
            //判断blockId是否已经 执行过请求区块数据
            if (requestBlockIds.getIfPresent(blockId) == null) {
              //缓存要请求区块的block
              requestBlockIds.put(blockId, System.currentTimeMillis());
              //把block加入 syncBlockRequested集合
              peer.getSyncBlockRequested().put(blockId, System.currentTimeMillis());
              //把block 加入peer 的待发送的列表里
              send.get(peer).add(blockId);

              //判断 peer 待发送列表不超过100 (如果超过100则等待定时任务下次执行)
              if (send.get(peer).size() >= MAX_BLOCK_FETCH_PER_PEER) {
                break;
              }
            }
          }
        });

    //遍历send 发送FetchInvDataMessage消息
    send.forEach((peer, blockIds) -> {
      if (!blockIds.isEmpty()) {
        peer.sendMessage(new FetchInvDataMessage(new LinkedList<>(blockIds), InventoryType.BLOCK));
      }
    });
  }

  private synchronized void handleSyncBlock() {

    synchronized (blockJustReceived) {
      blockWaitToProcess.putAll(blockJustReceived);
      blockJustReceived.clear();
    }

    final boolean[] isProcessed = {true};

    while (isProcessed[0]) {

      isProcessed[0] = false;

      synchronized (tronNetDelegate.getBlockLock()) {
        blockWaitToProcess.forEach((msg, peerConnection) -> {
          if (peerConnection.isDisconnect()) {
            blockWaitToProcess.remove(msg);
            invalid(msg.getBlockId());
            return;
          }
          final boolean[] isFound = {false};
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
            processSyncBlock(msg.getBlockCapsule());
          }
        });
      }
    }
  }

  private void processSyncBlock(BlockCapsule block) {
    boolean flag = true;
    BlockId blockId = block.getBlockId();
    try {
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
