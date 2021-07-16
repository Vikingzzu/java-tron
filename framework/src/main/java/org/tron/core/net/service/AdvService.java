package org.tron.core.net.service;

import static org.tron.core.config.Parameter.ChainConstant.BLOCK_PRODUCED_INTERVAL;
import static org.tron.core.config.Parameter.NetConstants.MAX_TRX_FETCH_PER_PEER;
import static org.tron.core.config.Parameter.NetConstants.MSG_CACHE_DURATION_IN_BLOCKS;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.common.overlay.discover.node.statistics.MessageCount;
import org.tron.common.overlay.message.Message;
import org.tron.common.utils.Sha256Hash;
import org.tron.common.utils.Time;
import org.tron.core.capsule.BlockCapsule.BlockId;
import org.tron.core.config.args.Args;
import org.tron.core.net.TronNetDelegate;
import org.tron.core.net.message.BlockMessage;
import org.tron.core.net.message.FetchInvDataMessage;
import org.tron.core.net.message.InventoryMessage;
import org.tron.core.net.message.TransactionMessage;
import org.tron.core.net.peer.Item;
import org.tron.core.net.peer.PeerConnection;
import org.tron.protos.Protocol.Inventory.InventoryType;

/**
 * 广播区块儿和交易service
 */
@Slf4j(topic = "net")
@Component
public class AdvService {
  
  private final int MAX_INV_TO_FETCH_CACHE_SIZE = 100_000;
  private final int MAX_TRX_CACHE_SIZE = 50_000;
  private final int MAX_BLOCK_CACHE_SIZE = 10;
  private final int MAX_SPREAD_SIZE = 1_000;

  @Autowired
  private TronNetDelegate tronNetDelegate;

  //放置接收到的清单消息的item  最终都是要发送 FETCH_INV_DATA消息（取回具体数据） 传播出去
  private ConcurrentHashMap<Item, Long> invToFetch = new ConcurrentHashMap<>();

  //收到的广播数据map 都是要发送清单消息传播出去
  private ConcurrentHashMap<Item, Long> invToSpread = new ConcurrentHashMap<>();

  //接收到的清单消息item缓存 1个小时失效
  private Cache<Item, Long> invToFetchCache = CacheBuilder.newBuilder()
      .maximumSize(MAX_INV_TO_FETCH_CACHE_SIZE).expireAfterWrite(1, TimeUnit.HOURS)
      .recordStats().build();

  //缓存收到交易数据 1小时后过期
  private Cache<Item, Message> trxCache = CacheBuilder.newBuilder()
      .maximumSize(MAX_TRX_CACHE_SIZE).expireAfterWrite(1, TimeUnit.HOURS)
      .recordStats().build();

  //缓存收到区块数据 1分钟后过期
  private Cache<Item, Message> blockCache = CacheBuilder.newBuilder()
      .maximumSize(MAX_BLOCK_CACHE_SIZE).expireAfterWrite(1, TimeUnit.MINUTES)
      .recordStats().build();

  private ScheduledExecutorService spreadExecutor = Executors.newSingleThreadScheduledExecutor();

  private ScheduledExecutorService fetchExecutor = Executors.newSingleThreadScheduledExecutor();

  @Getter
  //交易总数统计
  private MessageCount trxCount = new MessageCount();

  //配置了true  则只接收区块,不接收交易  该服务的主要功能就是为了和SR同步区块
  private boolean fastForward = Args.getInstance().isFastForward();

  //服务启动后init
  public void init() {

    //true的话不启动异步线程  正常节点为false
    if (fastForward) {
      return;
    }

    //启处理广播清单集合消息invToSpread  发送Inventory消息 线程
    spreadExecutor.scheduleWithFixedDelay(() -> {
      try {
        consumerInvToSpread();
      } catch (Exception exception) {
        logger.error("Spread thread error. {}", exception.getMessage());
      }
    }, 100, 30, TimeUnit.MILLISECONDS);

    //处理清单消息记录集合invToFetch  发送FetchInvDataMessage消息
    fetchExecutor.scheduleWithFixedDelay(() -> {
      try {
        consumerInvToFetch();
      } catch (Exception exception) {
        logger.error("Fetch thread error. {}", exception.getMessage());
      }
    }, 100, 30, TimeUnit.MILLISECONDS);
  }

  //关闭线程池
  public void close() {
    spreadExecutor.shutdown();
    fetchExecutor.shutdown();
  }

  //fastForward 节点更新缓存
  public synchronized void addInvToCache(Item item) {
    invToFetchCache.put(item, System.currentTimeMillis());
    invToFetch.remove(item);
  }

  //处理接收清单消息的item
  public boolean addInv(Item item) {
    //fastForward=true  表示该服务只接收区块  不接收交易
    if (fastForward && item.getType().equals(InventoryType.TRX)) {
      return false;
    }

    //交易缓存中存在该item 说明已经处理过该交易
    if (item.getType().equals(InventoryType.TRX) && trxCache.getIfPresent(item) != null) {
      return false;
    }
    //区块缓存中存在该item 说明已经处理过该区块
    if (item.getType().equals(InventoryType.BLOCK) && blockCache.getIfPresent(item) != null) {
      return false;
    }

    synchronized (this) {
      //判断缓存invToFetchCache 中是否存在该 item
      if (invToFetchCache.getIfPresent(item) != null) {
        return false;
      }
      //缓存invToFetchCache 记录该item
      invToFetchCache.put(item, System.currentTimeMillis());
      //放入到 invToFetch 待发送（FETCH_INV_DATA消息）列表里
      invToFetch.put(item, System.currentTimeMillis());
    }

    if (InventoryType.BLOCK.equals(item.getType())) {
      //处理清单消息记录集合 发送FetchInvDataMessage消息
      consumerInvToFetch();
    }

    return true;
  }

  //从缓存中获取消息
  public Message getMessage(Item item) {
    if (item.getType() == InventoryType.TRX) {
      return trxCache.getIfPresent(item);
    } else {
      return blockCache.getIfPresent(item);
    }
  }

  //广播消息
  public void broadcast(Message msg) {

    //单独的一块特殊逻辑（和SR同步区块儿的任务节点配置为ture） 正常节点为false
    if (fastForward) {
      return;
    }

    //判断收到广播数据map的数量是否大于最大的数量 1000
    if (invToSpread.size() > MAX_SPREAD_SIZE) {
      logger.warn("Drop message, type: {}, ID: {}.", msg.getType(), msg.getMessageId());
      return;
    }

    Item item;
    if (msg instanceof BlockMessage) {
      //判断消息为区块儿消息
      BlockMessage blockMsg = (BlockMessage) msg;
      //生成区块item
      item = new Item(blockMsg.getMessageId(), InventoryType.BLOCK);
      logger.info("Ready to broadcast block {}", blockMsg.getBlockId().getString());
      //遍历区块儿里的每一笔交易信息
      blockMsg.getBlockCapsule().getTransactions().forEach(transactionCapsule -> {
        Sha256Hash tid = transactionCapsule.getTransactionId();
        //广播数据map 中删除已经打包的交易
        invToSpread.remove(tid);
        //缓存区块儿中的交易信息（1小时后过期）
        trxCache.put(new Item(tid, InventoryType.TRX),
            new TransactionMessage(transactionCapsule.getInstance()));
      });
      //缓存区块儿信息（1分钟后过期）
      blockCache.put(item, msg);
    } else if (msg instanceof TransactionMessage) {
      //判断消息为交易消息
      TransactionMessage trxMsg = (TransactionMessage) msg;
      //生成交易item
      item = new Item(trxMsg.getMessageId(), InventoryType.TRX);
      //交易数量增加（上报交易监控指标）
      trxCount.add();
      //缓存交易信息（1小时后过期）
      trxCache.put(item, new TransactionMessage(trxMsg.getTransactionCapsule().getInstance()));
    } else {
      //其他类型的消息不支持广播
      logger.error("Adv item is neither block nor trx, type: {}", msg.getType());
      return;
    }

    //记录收到的广播数据
    invToSpread.put(item, System.currentTimeMillis());

    //如果是区块儿消息 则发送清单消息传播出去
    if (InventoryType.BLOCK.equals(item.getType())) {
      consumerInvToSpread();
    }
  }

  //广播BlockMessage
  public void fastForward(BlockMessage msg) {
    Item item = new Item(msg.getBlockId(), InventoryType.BLOCK);
    //找到 活跃的 且 不需要同步block 且 没有接收到item清单 且 没有广播过的item 的peer连接列表（fastForward节点逻辑）
    List<PeerConnection> peers = tronNetDelegate.getActivePeer().stream()
        .filter(peer -> !peer.isNeedSyncFromPeer() && !peer.isNeedSyncFromUs())
        .filter(peer -> peer.getAdvInvReceive().getIfPresent(item) == null
            && peer.getAdvInvSpread().getIfPresent(item) == null)
        .collect(Collectors.toList());

    if (!fastForward) {
      //如果不是fastForward 节点 则 找到 所有的fastForward列表（非fastForward节点逻辑）
      peers = peers.stream().filter(peer -> peer.isFastForwardPeer()).collect(Collectors.toList());
    }

    peers.forEach(peer -> {
      //发送block消息
      peer.fastSend(msg);
      //缓存广播过的 block
      peer.getAdvInvSpread().put(item, System.currentTimeMillis());
      //设置fastForwardBlock
      peer.setFastForwardBlock(msg.getBlockId());
    });
  }


  public void onDisconnect(PeerConnection peer) {
    if (!peer.getAdvInvRequest().isEmpty()) {
      peer.getAdvInvRequest().keySet().forEach(item -> {
        if (tronNetDelegate.getActivePeer().stream()
            .anyMatch(p -> !p.equals(peer) && p.getAdvInvReceive().getIfPresent(item) != null)) {
          invToFetch.put(item, System.currentTimeMillis());
        } else {
          invToFetchCache.invalidate(item);
        }
      });
    }

    if (invToFetch.size() > 0) {
      consumerInvToFetch();
    }
  }

  //处理清单消息记录集合invToFetch  发送FetchInvDataMessage消息
  private void consumerInvToFetch() {
    //拿到建立连接 且不需要同步区块的连接
    Collection<PeerConnection> peers = tronNetDelegate.getActivePeer().stream()
        .filter(peer -> peer.isIdle())
        .collect(Collectors.toList());

    InvSender invSender = new InvSender();
    long now = System.currentTimeMillis();
    synchronized (this) {
      //如果待发送列表为空  则返回
      if (invToFetch.isEmpty() || peers.isEmpty()) {
        return;
      }
      invToFetch.forEach((item, time) -> {
        //如果处理该item 超过15s 则直接放弃处理该item
        if (time < now - MSG_CACHE_DURATION_IN_BLOCKS * BLOCK_PRODUCED_INTERVAL) {
          logger.info("This obj is too late to fetch, type: {} hash: {}.", item.getType(),
                  item.getHash());
          //处理队列删除
          invToFetch.remove(item);
          //缓存删除
          invToFetchCache.invalidate(item);
          return;
        }

        //在收到的清单缓存中 且 peer的大小小于1000
        peers.stream().filter(peer -> peer.getAdvInvReceive().getIfPresent(item) != null
                && invSender.getSize(peer) < MAX_TRX_FETCH_PER_PEER)
                .sorted(Comparator.comparingInt(peer -> invSender.getSize(peer)))
                .findFirst().ifPresent(peer -> {
                  //添加发送任务
                  invSender.add(item, peer);
                  //发送任务入缓存
                  peer.getAdvInvRequest().put(item, now);
                  //处理队列删除
                  invToFetch.remove(item);
                });
      });
    }

    //发送FetchInvDataMessage 消息
    invSender.sendFetch();
  }

  //处理广播清单集合消息invToSpread  发送Inventory消息
  private synchronized void consumerInvToSpread() {
    //拿到建立连接 且不需要同步区块的连接
    List<PeerConnection> peers = tronNetDelegate.getActivePeer().stream()
        .filter(peer -> !peer.isNeedSyncFromPeer() && !peer.isNeedSyncFromUs())
        .collect(Collectors.toList());

    //判断待发送消息数是否为空
    if (invToSpread.isEmpty() || peers.isEmpty()) {
      return;
    }

    InvSender invSender = new InvSender();

    invToSpread.forEach((item, time) -> peers.forEach(peer -> {
      if (peer.getAdvInvReceive().getIfPresent(item) == null
          && peer.getAdvInvSpread().getIfPresent(item) == null
              //如果item消息为区块儿消息 但 当前的时间距出块时间大于3s了 则认为其他的节点已经处理了该区块儿   就不需要把该区块儿继续传递下去
          && !(item.getType().equals(InventoryType.BLOCK)
          && System.currentTimeMillis() - time > BLOCK_PRODUCED_INTERVAL)) {
        //Connection中缓存加入任务的item
        peer.getAdvInvSpread().put(item, Time.getCurrentMillis());
        invSender.add(item, peer);
      }
      //删除已经加入清单的item
      invToSpread.remove(item);
    }));

    //发送Inventory（清单消息）
    invSender.sendInv();
  }

  //清单消息发送sender
  class InvSender {

    // InventoryType-库存-（TRX = 0;BLOCK = 1）
    private HashMap<PeerConnection, HashMap<InventoryType, LinkedList<Sha256Hash>>> send
        = new HashMap<>();

    public void clear() {
      this.send.clear();
    }

    public void add(Entry<Sha256Hash, InventoryType> id, PeerConnection peer) {
      //存在连接 且 该连接不包含该id的InventoryType
      if (send.containsKey(peer) && !send.get(peer).containsKey(id.getValue())) {
        send.get(peer).put(id.getValue(), new LinkedList<>());
      } else if (!send.containsKey(peer)) {
        //不存在连接
        send.put(peer, new HashMap<>());
        send.get(peer).put(id.getValue(), new LinkedList<>());
      }
      //添加该元素的值
      send.get(peer).get(id.getValue()).offer(id.getKey());
    }

    public void add(Item id, PeerConnection peer) {
      //存在连接 且 该连接不包含该id的InventoryType
      if (send.containsKey(peer) && !send.get(peer).containsKey(id.getType())) {
        send.get(peer).put(id.getType(), new LinkedList<>());
      } else if (!send.containsKey(peer)) {
        //不存在连接
        send.put(peer, new HashMap<>());
        send.get(peer).put(id.getType(), new LinkedList<>());
      }
      //添加该元素的hash值
      send.get(peer).get(id.getType()).offer(id.getHash());
    }

    public int getSize(PeerConnection peer) {
      if (send.containsKey(peer)) {
        return send.get(peer).values().stream().mapToInt(LinkedList::size).sum();
      }
      return 0;
    }

    //发送清单消息 Inventory
    public void sendInv() {
      send.forEach((peer, ids) -> ids.forEach((key, value) -> {
        //FastForward节点只广播区块儿数据    不广播交易数据
        if (peer.isFastForwardPeer() && key.equals(InventoryType.TRX)) {
          return;
        }
        if (key.equals(InventoryType.BLOCK)) {
          //区块排序
          value.sort(Comparator.comparingLong(value1 -> new BlockId(value1).getNum()));
          peer.fastSend(new InventoryMessage(value, key));
        } else {
          peer.sendMessage(new InventoryMessage(value, key));
        }
      }));
    }

    //发送FetchInvDataMessage 消息
    void sendFetch() {
      send.forEach((peer, ids) -> ids.forEach((key, value) -> {
        if (key.equals(InventoryType.BLOCK)) {
          value.sort(Comparator.comparingLong(value1 -> new BlockId(value1).getNum()));
          peer.fastSend(new FetchInvDataMessage(value, key));
        } else {
          peer.sendMessage(new FetchInvDataMessage(value, key));
        }
      }));
    }
  }

}
