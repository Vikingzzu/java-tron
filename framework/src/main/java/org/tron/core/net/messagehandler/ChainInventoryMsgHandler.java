package org.tron.core.net.messagehandler;

import static org.tron.core.config.Parameter.ChainConstant.BLOCK_PRODUCED_INTERVAL;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
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

/**
 * 处理接收到的区块儿链清单消息handler
 */
@Slf4j(topic = "net")
@Component
public class ChainInventoryMsgHandler implements TronMsgHandler {

  @Autowired
  private TronNetDelegate tronNetDelegate;

  @Autowired
  private SyncService syncService;

  @Override
  //处理消息
  public void processMessage(PeerConnection peer, TronMessage msg) throws P2pException {

    //msg转化为ChainInventoryMessage（链清单消息）
    ChainInventoryMessage chainInventoryMessage = (ChainInventoryMessage) msg;

    //校验消息
    check(peer, chainInventoryMessage);

    //设置需要从对方同步区块状态
    peer.setNeedSyncFromPeer(true);

    //已经得到peer链同步的节点 清空之前本节点生成的摘要信息
    peer.setSyncChainRequested(null);

    Deque<BlockId> blockIdWeGet = new LinkedList<>(chainInventoryMessage.getBlockIds());

    //如果得到的block列表只有1个  且这一个在我们链中存在  说明 链长度持平   不需要同步
    if (blockIdWeGet.size() == 1 && tronNetDelegate.containBlock(blockIdWeGet.peek())) {
      peer.setNeedSyncFromPeer(false);
      return;
    }

    //eg:     syncBlockToFetch:5->6->7->8->9  blockIdWeGet:7->8->9->10->11
    //result  syncBlockToFetch:5->6->7
    while (!peer.getSyncBlockToFetch().isEmpty()) {
      if (peer.getSyncBlockToFetch().peekLast().equals(blockIdWeGet.peekFirst())) {
        break;
      }
      peer.getSyncBlockToFetch().pollLast();
    }

    //blockIdWeGet:8->9->10->11
    blockIdWeGet.poll();

    //记录RemainNum
    peer.setRemainNum(chainInventoryMessage.getRemainNum());

    //最终要生成的 syncBlockToFetch： 5->6->7->8->9->10->11
    peer.getSyncBlockToFetch().addAll(blockIdWeGet);

    synchronized (tronNetDelegate.getBlockLock()) {
      //从syncBlockToFetch的头部节点开始遍历  删除已经在存在的Block 并把删除的block更新到 blockBothHave
      while (!peer.getSyncBlockToFetch().isEmpty() && tronNetDelegate
          .containBlock(peer.getSyncBlockToFetch().peek())) {
        BlockId blockId = peer.getSyncBlockToFetch().pop();
        peer.setBlockBothHave(blockId);
        logger.info("Block {} from {} is processed", blockId.getString(), peer.getNode().getHost());
      }
    }

    //待更新的RemainNum为空 且 syncBlockToFetch不为空    或者是   待更新的RemainNum不为空 但 syncBlockToFetch的大小超过2000
    if ((chainInventoryMessage.getRemainNum() == 0 && !peer.getSyncBlockToFetch().isEmpty())
        || (chainInventoryMessage.getRemainNum() != 0
        && peer.getSyncBlockToFetch().size() > NetConstants.SYNC_FETCH_BATCH_NUM)) {
      //设置fetchFlag=true  此时异步线程 根据syncBlockToFetch列表 开始同步具体的区块数据
      syncService.setFetchFlag(true);
    } else {
      //否则继续发起同步  发送SyncBlockChainMessage消息 带上自己的摘要信息
      syncService.syncNext(peer);
    }
  }

  //校验消息
  private void check(PeerConnection peer, ChainInventoryMessage msg) throws P2pException {
    //检查当前peer是否储存了 链摘要信息（设置的过程在p2p网络握手完毕后发起同步区块消息时设置 ---- SyncService -> peer.setSyncChainRequested 117行）
    if (peer.getSyncChainRequested() == null) {
      throw new P2pException(TypeEnum.BAD_MESSAGE, "not send syncBlockChainMsg");
    }

    //拿到要同步的Block块集合判断
    List<BlockId> blockIds = msg.getBlockIds();
    if (CollectionUtils.isEmpty(blockIds)) {
      throw new P2pException(TypeEnum.BAD_MESSAGE, "blockIds is empty");
    }

    //每次要同步的区块个数不能大于2000个 （此处在peer节点生成blockIds时做了限制  ---- SyncBlockChainMsgHandler -> Math.min() 115行）
    if (blockIds.size() > NetConstants.SYNC_FETCH_BATCH_NUM + 1) {
      throw new P2pException(TypeEnum.BAD_MESSAGE, "big blockIds size: " + blockIds.size());
    }

    //如果blockIds的个数小于2000个，说明peer节点的区块已经全部同步过来了    要是这一次没有同步完 那么RemainNum肯定大于0
    // （此处逻辑在 SyncBlockChainMsgHandler -> remainNum = tronNetDelegate.getHeadBlockId().getNum() - blockIds.peekLast().getNum() 54行）
    if (msg.getRemainNum() != 0 && blockIds.size() < NetConstants.SYNC_FETCH_BATCH_NUM) {
      throw new P2pException(TypeEnum.BAD_MESSAGE,
          "remain: " + msg.getRemainNum() + ", blockIds size: " + blockIds.size());
    }

    //判断同步过来的区块的有序递增和连续性
    long num = blockIds.get(0).getNum();
    for (BlockId id : msg.getBlockIds()) {
      if (id.getNum() != num++) {
        throw new P2pException(TypeEnum.BAD_MESSAGE, "not continuous block");
      }
    }

    //判断 通过过来的区块链信息的第一个blockId 是不是在摘要信息中存在（通过过来的区块链要以摘要信息结尾节点的block开始）
    // 设置摘要信息的代码 SyncService -> peer.setSyncChainRequested 117行
    if (!peer.getSyncChainRequested().getKey().contains(blockIds.get(0))) {
      throw new P2pException(TypeEnum.BAD_MESSAGE, "unlinked block, my head: "
          + peer.getSyncChainRequested().getKey().getLast().getString()
          + ", peer: " + blockIds.get(0).getString());
    }

    if (tronNetDelegate.getHeadBlockId().getNum() > 0) {
      //计算us的 当前时间-上个固化块的时间 可以得出 本节点 大致还需要同步的时间范围（从上个固化块的时间-->到-->当前时间 的块儿需要同步 允许有1个小时的时间误差）
      long maxRemainTime =
          ChainConstant.CLOCK_MAX_DELAY + System.currentTimeMillis() - tronNetDelegate
              .getBlockTime(tronNetDelegate.getSolidBlockId());
      //计算还需要同步的区块的个数 得到 预计本节点同步后的高度num（当前时间预计同步后的高度）
      long maxFutureNum =
          maxRemainTime / BLOCK_PRODUCED_INTERVAL + tronNetDelegate.getSolidBlockId().getNum();
      long lastNum = blockIds.get(blockIds.size() - 1).getNum();
      //比较peer链的高度num 大于 预估的高度 则说明peer节点有问题 （中间已经有了1个小时的误差）
      if (lastNum + msg.getRemainNum() > maxFutureNum) {
        throw new P2pException(TypeEnum.BAD_MESSAGE, "lastNum: " + lastNum + " + remainNum: "
            + msg.getRemainNum() + " > futureMaxNum: " + maxFutureNum);
      }
    }
  }

}
