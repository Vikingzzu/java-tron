package org.tron.core.net;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.common.overlay.message.Message;
import org.tron.common.overlay.server.ChannelManager;
import org.tron.core.exception.P2pException;
import org.tron.core.exception.P2pException.TypeEnum;
import org.tron.core.net.message.BlockMessage;
import org.tron.core.net.message.TronMessage;
import org.tron.core.net.messagehandler.BlockMsgHandler;
import org.tron.core.net.messagehandler.ChainInventoryMsgHandler;
import org.tron.core.net.messagehandler.FetchInvDataMsgHandler;
import org.tron.core.net.messagehandler.InventoryMsgHandler;
import org.tron.core.net.messagehandler.PbftDataSyncHandler;
import org.tron.core.net.messagehandler.SyncBlockChainMsgHandler;
import org.tron.core.net.messagehandler.TransactionsMsgHandler;
import org.tron.core.net.peer.PeerConnection;
import org.tron.core.net.peer.PeerStatusCheck;
import org.tron.core.net.service.AdvService;
import org.tron.core.net.service.SyncService;
import org.tron.protos.Protocol.ReasonCode;

/**
 * tron 网络 服务
 * eg:p2p网络功能管理service 消息广播
 * eg:onMessage 接收各种协议的消息
 */
@Slf4j(topic = "net")
@Component
public class TronNetService {

  @Autowired
  private ChannelManager channelManager;

  @Autowired
  private AdvService advService;

  @Autowired
  private SyncService syncService;

  @Autowired
  private PeerStatusCheck peerStatusCheck;

  @Autowired
  private SyncBlockChainMsgHandler syncBlockChainMsgHandler;

  @Autowired
  private ChainInventoryMsgHandler chainInventoryMsgHandler;

  @Autowired
  private InventoryMsgHandler inventoryMsgHandler;


  @Autowired
  private FetchInvDataMsgHandler fetchInvDataMsgHandler;

  @Autowired
  private BlockMsgHandler blockMsgHandler;

  @Autowired
  private TransactionsMsgHandler transactionsMsgHandler;

  @Autowired
  private PbftDataSyncHandler pbftDataSyncHandler;

  public void start() {
    channelManager.init();
    advService.init();
    //异步线程同步服务 初始化
    syncService.init();
    peerStatusCheck.init();
    transactionsMsgHandler.init();
    logger.info("TronNetService start successfully.");
  }

  public void stop() {
    channelManager.close();
    advService.close();
    syncService.close();
    peerStatusCheck.close();
    transactionsMsgHandler.close();
    logger.info("TronNetService closed successfully.");
  }

  //广播消息
  public void broadcast(Message msg) {
    advService.broadcast(msg);
  }

  public void fastForward(BlockMessage msg) {
    advService.fastForward(msg);
  }

  //处理接收到的各种不同类型的消息
  protected void onMessage(PeerConnection peer, TronMessage msg) {
    try {
      switch (msg.getType()) {
        /**
         * 异步 区块 链（p2p握手后发起同步区块儿消息）
         * p2p连接建立之后，通过peer和us的比较，决定是否需要同步区块儿， 如果需要同步区块儿则 计算本链的BlockChainSummary（区块链摘要）
         * 然后发送SYNC_BLOCK_CHAIN消息 给远端节点
         */
        case SYNC_BLOCK_CHAIN:
          syncBlockChainMsgHandler.processMessage(peer, msg);
          break;
        /**
         * 区块 链 清单（接到SYNC_BLOCK_CHAIN消息后 打包对方需要的blocks返回 区块链清单消息）
         * 接收到SYNC_BLOCK_CHAIN消息后  我们节点 计算出需要同步给对方的区块 和 还需要同步的追平数量  new ChainInventoryMessage(blockIds, remainNum)
         * 然后发送BLOCK_CHAIN_INVENTORY消息给原来的节点
         */
        case BLOCK_CHAIN_INVENTORY:
          chainInventoryMsgHandler.processMessage(peer, msg);
          break;
          //清单
        case INVENTORY:
          inventoryMsgHandler.processMessage(peer, msg);
          break;
        /**
         * 取回 环境 数据（取回具体的区块数据）(定时任务发起定时轮询 us中需要从peer同步的区块集合syncBlockToFetch 然后发送FETCH_INV_DATA 消息请求具体的区块数据)
         * 1：us节点接到BLOCK_CHAIN_INVENTORY消息后  可以拿到具体的需要同步的blocks集合  存储在syncBlockToFetch中
         * 2：然后由定时任务发起定时轮询，拿到syncBlockToFetch集合里的数据 封装后向peer发送 FETCH_INV_DATA消息 取回具体的区块
         * code ---->  SyncService -> FetchInvDataMessage(new LinkedList<>(blockIds), InventoryType.BLOCK))  251行
         */
        case FETCH_INV_DATA:
          fetchInvDataMsgHandler.processMessage(peer, msg);
          break;
          //区块
        case BLOCK:
          blockMsgHandler.processMessage(peer, msg);
          break;
          //交易
        case TRXS:
          transactionsMsgHandler.processMessage(peer, msg);
          break;
          //PBFT(共识算法) 提交 消息
        case PBFT_COMMIT_MSG:
          pbftDataSyncHandler.processMessage(peer, msg);
          break;
          //其他消息
        default:
          throw new P2pException(TypeEnum.NO_SUCH_MESSAGE, msg.getType().toString());
      }
    } catch (Exception e) {
      //处理不同异常的逻辑
      processException(peer, msg, e);
    }
  }

  //处理不同异常的逻辑
  private void processException(PeerConnection peer, TronMessage msg, Exception ex) {
    ReasonCode code;

    if (ex instanceof P2pException) {
      TypeEnum type = ((P2pException) ex).getType();
      switch (type) {
        case BAD_TRX:
          code = ReasonCode.BAD_TX;
          break;
        case BAD_BLOCK:
          code = ReasonCode.BAD_BLOCK;
          break;
        case NO_SUCH_MESSAGE:
        case MESSAGE_WITH_WRONG_LENGTH:
        case BAD_MESSAGE:
          code = ReasonCode.BAD_PROTOCOL;
          break;
        case SYNC_FAILED:
          code = ReasonCode.SYNC_FAIL;
          break;
        case UNLINK_BLOCK:
          code = ReasonCode.UNLINKABLE;
          break;
        default:
          code = ReasonCode.UNKNOWN;
          break;
      }
      logger.error("Message from {} process failed, {} \n type: {}, detail: {}.",
          peer.getInetAddress(), msg, type, ex.getMessage());
    } else {
      code = ReasonCode.UNKNOWN;
      logger.error("Message from {} process failed, {}",
          peer.getInetAddress(), msg, ex);
    }

    peer.disconnect(code);
  }
}
