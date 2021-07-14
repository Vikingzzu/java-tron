package org.tron.core.net.peer;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.Deque;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.tron.common.overlay.message.HelloMessage;
import org.tron.common.overlay.message.Message;
import org.tron.common.overlay.server.Channel;
import org.tron.common.utils.Pair;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.Constant;
import org.tron.core.capsule.BlockCapsule.BlockId;
import org.tron.core.config.Parameter.NetConstants;
import org.tron.core.net.TronNetDelegate;
import org.tron.core.net.service.AdvService;
import org.tron.core.net.service.SyncService;

/**
 * p2p网络节点连接信息
 */
@Slf4j(topic = "net")
@Component
@Scope("prototype")
public class PeerConnection extends Channel {

  @Autowired
  private TronNetDelegate tronNetDelegate;

  @Autowired
  private SyncService syncService;

  @Autowired
  private AdvService advService;

  @Setter
  @Getter
  private HelloMessage helloMessage;

  private int invCacheSize = 100_000;

  @Setter
  @Getter
  private Cache<Item, Long> advInvReceive = CacheBuilder.newBuilder().maximumSize(invCacheSize)
      .expireAfterWrite(1, TimeUnit.HOURS).recordStats().build();

  @Setter
  @Getter
  private Cache<Item, Long> advInvSpread = CacheBuilder.newBuilder().maximumSize(invCacheSize)
      .expireAfterWrite(1, TimeUnit.HOURS).recordStats().build();

  @Setter
  @Getter
  private Map<Item, Long> advInvRequest = new ConcurrentHashMap<>();

  @Setter
  private BlockId fastForwardBlock;

  @Getter
  private BlockId blockBothHave = new BlockId();
  @Getter
  private volatile long blockBothHaveUpdateTime = System.currentTimeMillis();
  @Setter
  @Getter
  private BlockId lastSyncBlockId;
  @Setter
  @Getter
  private volatile long remainNum;
  @Getter
  private Cache<Sha256Hash, Long> syncBlockIdCache = CacheBuilder.newBuilder()
      .maximumSize(2 * NetConstants.SYNC_FETCH_BATCH_NUM).recordStats().build();
  @Setter
  @Getter
  //us需要同步的的block列表 从小打到 有序排列
  private Deque<BlockId> syncBlockToFetch = new ConcurrentLinkedDeque<>();
  @Setter
  @Getter
  //请求区块具体数据的Block集合
  private Map<BlockId, Long> syncBlockRequested = new ConcurrentHashMap<>();
  @Setter
  @Getter
  //储存当前peer 同步摘要内容信息
  private Pair<Deque<BlockId>, Long> syncChainRequested = null;
  @Setter
  @Getter
  private Set<BlockId> syncBlockInProcess = new HashSet<>();
  @Setter
  @Getter
  //代表本节点的区块信息不是最新的  需要从对方节点同步区块
  private volatile boolean needSyncFromPeer = true;
  @Setter
  @Getter
  //代表本节点对区块是最新的    对方节点需要同步本节点的区块
  private volatile boolean needSyncFromUs = true;

  public void setBlockBothHave(BlockId blockId) {
    this.blockBothHave = blockId;
    this.blockBothHaveUpdateTime = System.currentTimeMillis();
  }

  public boolean isIdle() {
    return advInvRequest.isEmpty() && syncBlockRequested.isEmpty() && syncChainRequested == null;
  }

  //发送tcp消息到队列
  public void sendMessage(Message message) {
    msgQueue.sendMessage(message);
  }

  //发送广播数据（一般为发送区块儿）
  public void fastSend(Message message) {
    msgQueue.fastSend(message);
  }

  //Connection建立连接
  public void onConnect() {
    long headBlockNum = tronNetDelegate.getHeadBlockId().getNum();
    long peerHeadBlockNum = getHelloMessage().getHeadBlockId().getNum();

    //判断HelloMessage中的head头区块儿num 大于 数据库主链中的head头区块儿num 则说明需要同步区块儿
    if (peerHeadBlockNum > headBlockNum) {
      //需要从对方同步区块儿 peer=true  us=false
      needSyncFromUs = false;
      //设置channel 状态 需要从对方同步区块儿 SYNCING
      setTronState(TronState.SYNCING);

      //发起区块儿同步
      syncService.startSync(this);
    } else {
      //不需要从对方同步区块儿 peer=false
      needSyncFromPeer = false;
      //如果区块儿num相等则不需要互相同步 us=false   否则的话说明对方需要从我们这同步区块儿 us=true
      if (peerHeadBlockNum == headBlockNum) {
        needSyncFromUs = false;
      }
      //设置channel 状态 对方需要从本节点同步区块儿 SYNC_COMPLETED
      setTronState(TronState.SYNC_COMPLETED);
    }
  }

  //Connection关闭连接
  public void onDisconnect() {
    syncService.onDisconnect(this);
    advService.onDisconnect(this);
    advInvReceive.cleanUp();
    advInvSpread.cleanUp();
    advInvRequest.clear();
    syncBlockIdCache.cleanUp();
    syncBlockToFetch.clear();
    syncBlockRequested.clear();
    syncBlockInProcess.clear();
    syncBlockInProcess.clear();
  }

  public String log() {
    long now = System.currentTimeMillis();
    return String.format(
        "Peer %s [%8s]\n"
            + "ping msg: count %d, max-average-min-last: %d %d %d %d\n"
            + "connect time: %ds\n"
            + "last know block num: %s\n"
            + "needSyncFromPeer:%b\n"
            + "needSyncFromUs:%b\n"
            + "syncToFetchSize:%d\n"
            + "syncToFetchSizePeekNum:%d\n"
            + "syncBlockRequestedSize:%d\n"
            + "remainNum:%d\n"
            + "syncChainRequested:%d\n"
            + "blockInProcess:%d\n",
        getNode().getHost() + ":" + getNode().getPort(),
        getNode().getHexIdShort(),

        getNodeStatistics().pingMessageLatency.getCount(),
        getNodeStatistics().pingMessageLatency.getMax(),
        getNodeStatistics().pingMessageLatency.getAvg(),
        getNodeStatistics().pingMessageLatency.getMin(),
        getNodeStatistics().pingMessageLatency.getLast(),

        (now - getStartTime()) / Constant.ONE_THOUSAND,
        fastForwardBlock != null ? fastForwardBlock.getNum() : blockBothHave.getNum(),
        isNeedSyncFromPeer(),
        isNeedSyncFromUs(),
        syncBlockToFetch.size(),
        !syncBlockToFetch.isEmpty() ? syncBlockToFetch.peek().getNum() : -1,
        syncBlockRequested.size(),
        remainNum,
        syncChainRequested == null ? 0 : (now - syncChainRequested.getValue()) 
                / Constant.ONE_THOUSAND,
        syncBlockInProcess.size())
        + nodeStatistics.toString() + "\n";
  }

  public boolean isSyncFinish() {
    return !(needSyncFromPeer || needSyncFromUs);
  }

}
