package org.tron.core.net.messagehandler;

import java.util.LinkedList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.core.capsule.BlockCapsule.BlockId;
import org.tron.core.config.Parameter.NetConstants;
import org.tron.core.exception.P2pException;
import org.tron.core.exception.P2pException.TypeEnum;
import org.tron.core.net.TronNetDelegate;
import org.tron.core.net.message.ChainInventoryMessage;
import org.tron.core.net.message.SyncBlockChainMessage;
import org.tron.core.net.message.TronMessage;
import org.tron.core.net.peer.PeerConnection;

/**
 * 接收到同步摘要处理handler
 */
@Slf4j(topic = "net")
@Component
public class SyncBlockChainMsgHandler implements TronMsgHandler {

  @Autowired
  private TronNetDelegate tronNetDelegate;

  @Override
  //处理消息
  public void processMessage(PeerConnection peer, TronMessage msg) throws P2pException {

    //msg转化成SyncBlockChainMessage
    SyncBlockChainMessage syncBlockChainMessage = (SyncBlockChainMessage) msg;

    //消息校验
    check(peer, syncBlockChainMessage);

    long remainNum = 0;

    //对方节点摘要block
    List<BlockId> summaryChainIds = syncBlockChainMessage.getBlockIds();

    //计算需要同的BlockId集合
    LinkedList<BlockId> blockIds = getLostBlockIds(summaryChainIds);

    if (blockIds.size() == 1) {
      //1个说明长度相同   不需要从我们这同步 us=false
      peer.setNeedSyncFromUs(false);
    } else {
      //需要从我们同步区块 us=true
      peer.setNeedSyncFromUs(true);
      //这次同步过后还需要同步的block的个数（还差这个值才能追平）
      remainNum = tronNetDelegate.getHeadBlockId().getNum() - blockIds.peekLast().getNum();
    }

    //设置这次同步的blockId的最大节点值
    peer.setLastSyncBlockId(blockIds.peekLast());
    //设置还需要追平的值
    peer.setRemainNum(remainNum);
    //发送ChainInventoryMessage消息（附带 这次同步的区块儿 和  还需要同步的区块数量）
    peer.sendMessage(new ChainInventoryMessage(blockIds, remainNum));
  }

  //消息校验
  private void check(PeerConnection peer, SyncBlockChainMessage msg) throws P2pException {
    //判断blockIds不能为空
    List<BlockId> blockIds = msg.getBlockIds();
    if (CollectionUtils.isEmpty(blockIds)) {
      throw new P2pException(TypeEnum.BAD_MESSAGE, "SyncBlockChain blockIds is empty");
    }

    //校验主链中是否含有 同步链头部节点blockId
    BlockId firstId = blockIds.get(0);
    if (!tronNetDelegate.containBlockInMainChain(firstId)) {
      throw new P2pException(TypeEnum.BAD_MESSAGE, "No first block:" + firstId.getString());
    }

    //校验主链最新的节点num是否大于 同步链头部节点num ？ 校验通过 ： 不通过，中间链部分缺失
    long headNum = tronNetDelegate.getHeadBlockId().getNum();
    if (firstId.getNum() > headNum) {
      throw new P2pException(TypeEnum.BAD_MESSAGE,
          "First blockNum:" + firstId.getNum() + " gt my head BlockNum:" + headNum);
    }

    //上次同步完成的最大节点block
    BlockId lastSyncBlockId = peer.getLastSyncBlockId();
    //同步链尾部节点num
    long lastNum = blockIds.get(blockIds.size() - 1).getNum();
    //校验 上次同步完成的最大节点block的num如果大于 本次同步链尾部最大节点num  则报异常  说明上次已经同步过了 这次同步不可信
    if (lastSyncBlockId != null && lastSyncBlockId.getNum() > lastNum) {
      throw new P2pException(TypeEnum.BAD_MESSAGE,
          "lastSyncNum:" + lastSyncBlockId.getNum() + " gt lastNum:" + lastNum);
    }
  }

  //计算需要同的BlockId集合
  private LinkedList<BlockId> getLostBlockIds(List<BlockId> blockIds) throws P2pException {

    BlockId unForkId = null;
    //从摘要链尾部节点开始遍历 找出主链上最小需要同步的BlockId位置
    for (int i = blockIds.size() - 1; i >= 0; i--) {
      if (tronNetDelegate.containBlockInMainChain(blockIds.get(i))) {
        unForkId = blockIds.get(i);
        break;
      }
    }

    //如果所有的摘要链block均在数据库中找不到  那说明没有交集 不同步
    if (unForkId == null) {
      throw new P2pException(TypeEnum.SYNC_FAILED, "unForkId is null");
    }

    //主链最新节点  和  要同步的最小BlockId位置+每次最多同步2000个   进行比较  哪个小 就按照哪个为结束位置标志
    long len = Math.min(tronNetDelegate.getHeadBlockId().getNum(),
        unForkId.getNum() + NetConstants.SYNC_FETCH_BATCH_NUM);

    LinkedList<BlockId> ids = new LinkedList<>();
    //把要同步的区块id 放到集合中返回
    for (long i = unForkId.getNum(); i <= len; i++) {
      BlockId id = tronNetDelegate.getBlockIdByNum(i);
      ids.add(id);
    }
    return ids;
  }

}
