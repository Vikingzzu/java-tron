package org.tron.core.net.messagehandler;

import static org.tron.core.config.Parameter.ChainConstant.BLOCK_PRODUCED_INTERVAL;
import static org.tron.core.config.Parameter.ChainConstant.BLOCK_SIZE;

import lombok.extern.slf4j.Slf4j;
import org.spongycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.core.Constant;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.BlockCapsule.BlockId;
import org.tron.core.config.args.Args;
import org.tron.core.exception.P2pException;
import org.tron.core.exception.P2pException.TypeEnum;
import org.tron.core.net.TronNetDelegate;
import org.tron.core.net.message.BlockMessage;
import org.tron.core.net.message.TronMessage;
import org.tron.core.net.peer.Item;
import org.tron.core.net.peer.PeerConnection;
import org.tron.core.net.service.AdvService;
import org.tron.core.net.service.SyncService;
import org.tron.core.services.WitnessProductBlockService;
import org.tron.protos.Protocol.Inventory.InventoryType;

/**
 * 区块消息处理handler
 */
@Slf4j(topic = "net")
@Component
public class BlockMsgHandler implements TronMsgHandler {

  @Autowired
  private TronNetDelegate tronNetDelegate;

  @Autowired
  private AdvService advService;

  @Autowired
  private SyncService syncService;

  @Autowired
  private WitnessProductBlockService witnessProductBlockService;

  private int maxBlockSize = BLOCK_SIZE + Constant.ONE_THOUSAND;

  private boolean fastForward = Args.getInstance().isFastForward();

  @Override
  public void processMessage(PeerConnection peer, TronMessage msg) throws P2pException {

    BlockMessage blockMessage = (BlockMessage) msg;
    BlockId blockId = blockMessage.getBlockId();

    //判断当前handler 和 peer 都不是FastForward 节点时才校验消息
    if (!fastForward && !peer.isFastForwardPeer()) {
      //校验消息
      check(peer, blockMessage);
    }

    //判断本节点是否发送过同步block的请求(是的话 说明收到的块是同步块)
    if (peer.getSyncBlockRequested().containsKey(blockId)) {
      //同步block的请求中删除当前block（因为现在已经拿到block了）
      peer.getSyncBlockRequested().remove(blockId);

      //处理收到的区块msg数据
      syncService.processBlock(peer, blockMessage);
    } else {
      //说明收到的块是广播块
      //删除 清单消息集合advInvReceive 中 要同步的block
      Long time = peer.getAdvInvRequest().remove(new Item(blockId, InventoryType.BLOCK));
      long now = System.currentTimeMillis();
      long interval = blockId.getNum() - tronNetDelegate.getHeadBlockId().getNum();
      //接收到block后继续广播出去该区块的方法
      processBlock(peer, blockMessage.getBlockCapsule());
      logger.info(
          "Receive block/interval {}/{} from {} fetch/delay {}/{}ms, "
              + "txs/process {}/{}ms, witness: {}",
          blockId.getNum(),
          interval,
          peer.getInetAddress(),
          time == null ? 0 : now - time,
          now - blockMessage.getBlockCapsule().getTimeStamp(),
          ((BlockMessage) msg).getBlockCapsule().getTransactions().size(),
          System.currentTimeMillis() - now,
          Hex.toHexString(blockMessage.getBlockCapsule().getWitnessAddress().toByteArray()));
    }
  }

  //校验消息
  private void check(PeerConnection peer, BlockMessage msg) throws P2pException {
    Item item = new Item(msg.getBlockId(), InventoryType.BLOCK);
    //判断 syncBlockRequested集合 和  advInvRequest集合 都存在该block
    if (!peer.getSyncBlockRequested().containsKey(msg.getBlockId()) && !peer.getAdvInvRequest()
        .containsKey(item)) {
      throw new P2pException(TypeEnum.BAD_MESSAGE, "no request");
    }
    BlockCapsule blockCapsule = msg.getBlockCapsule();
    //判断block大小是否超限
    if (blockCapsule.getInstance().getSerializedSize() > maxBlockSize) {
      throw new P2pException(TypeEnum.BAD_MESSAGE, "block size over limit");
    }
    long gap = blockCapsule.getTimeStamp() - System.currentTimeMillis();
    //判断block header的生成时间  是否 大于当前时间 3s  大于3s的block 还未产生 所以该block 肯定是错误的block
    if (gap >= BLOCK_PRODUCED_INTERVAL) {
      throw new P2pException(TypeEnum.BAD_MESSAGE, "block time error");
    }
  }

  //接收到block后继续广播出去该区块的方法（接收到的广播块）
  private void processBlock(PeerConnection peer, BlockCapsule block) throws P2pException {
    BlockId blockId = block.getBlockId();
    //判断数据库中是否含有当前block的父节点 （没有的话不处理当前接收到的块，继续发起同步区块逻辑）
    if (!tronNetDelegate.containBlock(block.getParentBlockId())) {
      logger.warn("Get unlink block {} from {}, head is {}.", blockId.getString(),
          peer.getInetAddress(), tronNetDelegate.getHeadBlockId().getString());
      //继续发起同步  发送SyncBlockChainMessage消息 带上自己的摘要信息
      syncService.startSync(peer);
      return;
    }

    Item item = new Item(blockId, InventoryType.BLOCK);
    //fastForward 节点更新缓存
    if (fastForward || peer.isFastForwardPeer()) {
      peer.getAdvInvReceive().put(item, System.currentTimeMillis());
      advService.addInvToCache(item);
    }

    //fastForward 节点逻辑
    if (fastForward) {
      //如果接收到的block num 小于 主链上的header num 的值，则不做处理
      if (block.getNum() < tronNetDelegate.getHeadBlockId().getNum()) {
        logger.warn("Receive a low block {}, head {}",
            blockId.getString(), tronNetDelegate.getHeadBlockId().getString());
        return;
      }

      //校验block合法
      if (tronNetDelegate.validBlock(block)) {
        //广播BlockMessage
        advService.fastForward(new BlockMessage(block));
        //维护可信的地址InetAddress列表（就是维护27个SR节点的列表）
        tronNetDelegate.trustNode(peer);
      }
    }

    //区块入库方法
    tronNetDelegate.processBlock(block, false);

    //TODO-M ？
    witnessProductBlockService.validWitnessProductTwoBlock(block);

    //更新共有block
    tronNetDelegate.getActivePeer().forEach(p -> {
      if (p.getAdvInvReceive().getIfPresent(blockId) != null) {
        p.setBlockBothHave(blockId);
      }
    });

    //如果节点 不是fastForward 节点则继续广播该区块
    if (!fastForward) {
      //广播该区块
      advService.broadcast(new BlockMessage(block));
    }
  }

}
