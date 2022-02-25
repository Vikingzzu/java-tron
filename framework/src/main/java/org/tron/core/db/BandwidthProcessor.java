package org.tron.core.db;

import static org.tron.core.config.Parameter.ChainConstant.TRX_PRECISION;
import static org.tron.protos.Protocol.Transaction.Contract.ContractType.ShieldedTransferContract;
import static org.tron.protos.Protocol.Transaction.Contract.ContractType.TransferAssetContract;

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.Commons;
import org.tron.core.ChainBaseManager;
import org.tron.core.Constant;
import org.tron.core.capsule.AccountCapsule;
import org.tron.core.capsule.AssetIssueCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.exception.AccountResourceInsufficientException;
import org.tron.core.exception.ContractValidateException;
import org.tron.core.exception.TooBigTransactionResultException;
import org.tron.protos.Protocol.Transaction.Contract;
import org.tron.protos.contract.AssetIssueContractOuterClass.TransferAssetContract;
import org.tron.protos.contract.BalanceContract.TransferContract;

@Slf4j(topic = "DB")
public class BandwidthProcessor extends ResourceProcessor {

  private ChainBaseManager chainBaseManager;

  public BandwidthProcessor(ChainBaseManager chainBaseManager) {
    super(chainBaseManager.getDynamicPropertiesStore(), chainBaseManager.getAccountStore());
    this.chainBaseManager = chainBaseManager;
  }

  @Override
  public void updateUsage(AccountCapsule accountCapsule) {
    long now = chainBaseManager.getHeadSlot();
    updateUsage(accountCapsule, now);
  }

  private void updateUsage(AccountCapsule accountCapsule, long now) {
    long oldNetUsage = accountCapsule.getNetUsage();
    long latestConsumeTime = accountCapsule.getLatestConsumeTime();
    accountCapsule.setNetUsage(increase(oldNetUsage, 0, latestConsumeTime, now));
    long oldFreeNetUsage = accountCapsule.getFreeNetUsage();
    long latestConsumeFreeTime = accountCapsule.getLatestConsumeFreeTime();
    accountCapsule.setFreeNetUsage(increase(oldFreeNetUsage, 0, latestConsumeFreeTime, now));

    if (chainBaseManager.getDynamicPropertiesStore().getAllowSameTokenName() == 0) {
      Map<String, Long> assetMap = accountCapsule.getAssetMap();
      assetMap.forEach((assetName, balance) -> {
        long oldFreeAssetNetUsage = accountCapsule.getFreeAssetNetUsage(assetName);
        long latestAssetOperationTime = accountCapsule.getLatestAssetOperationTime(assetName);
        accountCapsule.putFreeAssetNetUsage(assetName,
            increase(oldFreeAssetNetUsage, 0, latestAssetOperationTime, now));
      });
    }
    Map<String, Long> assetMapV2 = accountCapsule.getAssetMapV2();
    assetMapV2.forEach((assetName, balance) -> {
      long oldFreeAssetNetUsage = accountCapsule.getFreeAssetNetUsageV2(assetName);
      long latestAssetOperationTime = accountCapsule.getLatestAssetOperationTimeV2(assetName);
      accountCapsule.putFreeAssetNetUsageV2(assetName,
          increase(oldFreeAssetNetUsage, 0, latestAssetOperationTime, now));
    });
  }

  //--->处理交易带宽账单  用户网络带宽账单（原生交易  TRC10代币交易）
  @Override
  public void consume(TransactionCapsule trx, TransactionTrace trace)
      throws ContractValidateException, AccountResourceInsufficientException,
      TooBigTransactionResultException {
    List<Contract> contracts = trx.getInstance().getRawData().getContractList();
    if (trx.getResultSerializedSize() > Constant.MAX_RESULT_SIZE_IN_TX * contracts.size()) {
      throw new TooBigTransactionResultException();
    }

    long bytesSize;

    //计算交易的字节码大小
    if (chainBaseManager.getDynamicPropertiesStore().supportVM()) {
      //
      bytesSize = trx.getInstance().toBuilder().clearRet().build().getSerializedSize();
    } else {
      //普通交易
      bytesSize = trx.getSerializedSize();
    }

    for (Contract contract : contracts) {
      //不处理匿名交易
      if (contract.getType() == ShieldedTransferContract) {
        continue;
      }
      //合约交易 需要补位 bytesSize需加上64
      if (chainBaseManager.getDynamicPropertiesStore().supportVM()) {
        bytesSize += Constant.MAX_RESULT_SIZE_IN_TX;
      }

      logger.debug("trxId {}, bandwidth cost: {}", trx.getTransactionId(), bytesSize);
      //设置带宽消耗
      trace.setNetBill(bytesSize, 0);
      //获取当前交易的发起地址
      byte[] address = TransactionCapsule.getOwner(contract);
      //获取账户信息
      AccountCapsule accountCapsule = chainBaseManager.getAccountStore().get(address);
      //账户为空直接异常返回
      if (accountCapsule == null) {
        throw new ContractValidateException("account does not exist");
      }
      //
      long now = chainBaseManager.getHeadSlot();

      //交易是否是创建账户交易
      if (contractCreateNewAccount(contract)) {
        //开户逻辑   质押TRX量够支持开户带宽  或  支付0.1TRX的带宽费用
        consumeForCreateNewAccount(accountCapsule, bytesSize, now, trace);
        continue;
      }

      //判断 TRC10 token 转账的带宽消耗
      if (contract.getType() == TransferAssetContract && useAssetAccountNet(contract,
          accountCapsule, now, bytesSize)) {
        continue;
      }

      //判断转账方用户 质押TRX获取的带宽是否够用
      if (useAccountNet(accountCapsule, bytesSize, now)) {
        continue;
      }

      //判断转账方用户 免费带宽是否够用
      if (useFreeNet(accountCapsule, bytesSize, now)) {
        continue;
      }

      //判断用户燃烧TRX获取的带宽是否够用
      if (useTransactionFee(accountCapsule, bytesSize, trace)) {
        continue;
      }

      long fee = chainBaseManager.getDynamicPropertiesStore().getTransactionFee() * bytesSize;
      throw new AccountResourceInsufficientException(
          "Account has insufficient bandwidth[" + bytesSize + "] and balance["
              + fee + "] to create new account");
    }
  }

  //判断用户燃烧TRX获取的带宽是否够用
  private boolean useTransactionFee(AccountCapsule accountCapsule, long bytes,
      TransactionTrace trace) {
    //获取获取带宽所需要燃烧的TRX数量
    //getTransactionFee = 字节（byte）单位的数据所需要的sun单位数
    long fee = chainBaseManager.getDynamicPropertiesStore().getTransactionFee() * bytes;
    if (consumeFeeForBandwidth(accountCapsule, fee)) {
      //设置消耗带宽的TRX
      trace.setNetBill(0, fee);
      //累积用户消耗获取带宽的TRX
      chainBaseManager.getDynamicPropertiesStore().addTotalTransactionCost(fee);
      return true;
    } else {
      return false;
    }
  }

  //创建新账户交易
  private void consumeForCreateNewAccount(AccountCapsule accountCapsule, long bytes,
      long now, TransactionTrace trace)
      throws AccountResourceInsufficientException {
    //判断质押的TRX带宽是否够支持开户的带宽消耗量
    boolean ret = consumeBandwidthForCreateNewAccount(accountCapsule, bytes, now);

    if (!ret) {
      //质押TRX的带宽不够支持开户   则消耗0.1TRX来支付开户带宽
      ret = consumeFeeForCreateNewAccount(accountCapsule, trace);
      if (!ret) {
        throw new AccountResourceInsufficientException();
      }
    }
  }

  //判断质押的TRX带宽是否够支持开户的带宽消耗量
  public boolean consumeBandwidthForCreateNewAccount(AccountCapsule accountCapsule, long bytes,
      long now) {

    long createNewAccountBandwidthRatio = chainBaseManager.getDynamicPropertiesStore()
        .getCreateNewAccountBandwidthRate();

    long netUsage = accountCapsule.getNetUsage();
    long latestConsumeTime = accountCapsule.getLatestConsumeTime();
    long netLimit = calculateGlobalNetLimit(accountCapsule);

    long newNetUsage = increase(netUsage, 0, latestConsumeTime, now);

    if (bytes * createNewAccountBandwidthRatio <= (netLimit - newNetUsage)) {
      latestConsumeTime = now;
      long latestOperationTime = chainBaseManager.getHeadBlockTimeStamp();
      newNetUsage = increase(newNetUsage, bytes * createNewAccountBandwidthRatio,
          latestConsumeTime, now);
      accountCapsule.setLatestConsumeTime(latestConsumeTime);
      accountCapsule.setLatestOperationTime(latestOperationTime);
      accountCapsule.setNetUsage(newNetUsage);
      chainBaseManager.getAccountStore().put(accountCapsule.createDbKey(), accountCapsule);
      return true;
    }
    return false;
  }

  //账户没有足够的通过质押TRX获得的带宽，会烧掉0.1个TRX来支付带宽费用
  public boolean consumeFeeForCreateNewAccount(AccountCapsule accountCapsule,
      TransactionTrace trace) {
    //开户带宽消耗0.1TRX
    long fee = chainBaseManager.getDynamicPropertiesStore().getCreateAccountFee();
    if (consumeFeeForNewAccount(accountCapsule, fee)) {
      //支付0.1TRX开户 同时消耗带宽置为0
      trace.setNetBillForCreateNewAccount(0, fee);
      chainBaseManager.getDynamicPropertiesStore().addTotalCreateAccountCost(fee);
      return true;
    } else {
      return false;
    }
  }

  //判断交易是否是创建账户交易
  public boolean contractCreateNewAccount(Contract contract) {
    AccountCapsule toAccount;
    switch (contract.getType()) {
      case AccountCreateContract:
        return true;
      case TransferContract:
        TransferContract transferContract;
        try {
          transferContract = contract.getParameter().unpack(TransferContract.class);
        } catch (Exception ex) {
          throw new RuntimeException(ex.getMessage());
        }
        toAccount =
            chainBaseManager.getAccountStore().get(transferContract.getToAddress().toByteArray());
        return toAccount == null;
      case TransferAssetContract:
        TransferAssetContract transferAssetContract;
        try {
          transferAssetContract = contract.getParameter().unpack(TransferAssetContract.class);
        } catch (Exception ex) {
          throw new RuntimeException(ex.getMessage());
        }
        toAccount = chainBaseManager.getAccountStore()
            .get(transferAssetContract.getToAddress().toByteArray());
        return toAccount == null;
      default:
        return false;
    }
  }

  //判断 TRC10 token 转账的带宽消耗
  private boolean useAssetAccountNet(Contract contract, AccountCapsule accountCapsule, long now,
      long bytes)
      throws ContractValidateException {

    ByteString assetName;
    try {
      //获取TRC10代币名称
      assetName = contract.getParameter().unpack(TransferAssetContract.class).getAssetName();
    } catch (Exception ex) {
      throw new RuntimeException(ex.getMessage());
    }

    AssetIssueCapsule assetIssueCapsule;
    AssetIssueCapsule assetIssueCapsuleV2;
    assetIssueCapsule = Commons.getAssetIssueStoreFinal(
        chainBaseManager.getDynamicPropertiesStore(),
        chainBaseManager.getAssetIssueStore(), chainBaseManager.getAssetIssueV2Store())
        .get(assetName.toByteArray());
    //查询TRC10代币信息
    if (assetIssueCapsule == null) {
      throw new ContractValidateException("asset does not exist");
    }

    String tokenName = ByteArray.toStr(assetName.toByteArray());
    String tokenID = assetIssueCapsule.getId();
    //如果发行Token方自己发起的转账 则直接校验他自己质押代币获取的带宽是否够用
    if (assetIssueCapsule.getOwnerAddress() == accountCapsule.getAddress()) {
      return useAccountNet(accountCapsule, bytes, now);
    }

    //验证 发行Token资产总的免费Bandwidth Points是否足够消耗
    //获取发行Token资产方 总的免费带宽
    long publicFreeAssetNetLimit = assetIssueCapsule.getPublicFreeAssetNetLimit();
    long publicFreeAssetNetUsage = assetIssueCapsule.getPublicFreeAssetNetUsage();
    long publicLatestFreeNetTime = assetIssueCapsule.getPublicLatestFreeNetTime();

    //获取发行Token资产方 已使用免费带宽
    long newPublicFreeAssetNetUsage = increase(publicFreeAssetNetUsage, 0,
        publicLatestFreeNetTime, now);

    if (bytes > (publicFreeAssetNetLimit - newPublicFreeAssetNetUsage)) {
      logger.debug("The " + tokenID + " public free bandwidth is not enough");
      return false;
    }

    //转账发起者的Token剩余免费Bandwidth Points是否足够消耗
    //获取转账方的剩余免费带宽
    long freeAssetNetLimit = assetIssueCapsule.getFreeAssetNetLimit();

    long freeAssetNetUsage;
    //用户 TRC10 代币 上次操作（转账）时间
    long latestAssetOperationTime;
    if (chainBaseManager.getDynamicPropertiesStore().getAllowSameTokenName() == 0) {
      freeAssetNetUsage = accountCapsule
          .getFreeAssetNetUsage(tokenName);
      latestAssetOperationTime = accountCapsule
          .getLatestAssetOperationTime(tokenName);
    } else {
      freeAssetNetUsage = accountCapsule.getFreeAssetNetUsageV2(tokenID);
      latestAssetOperationTime = accountCapsule.getLatestAssetOperationTimeV2(tokenID);
    }

    //获取转账方的已使用免费带宽
    long newFreeAssetNetUsage = increase(freeAssetNetUsage, 0,
        latestAssetOperationTime, now);

    if (bytes > (freeAssetNetLimit - newFreeAssetNetUsage)) {
      logger.debug("The " + tokenID + " free bandwidth is not enough");
      return false;
    }

    //判断 Token发行者方 质押TRX获取Bandwidth Points剩余量是否足够消耗
    AccountCapsule issuerAccountCapsule = chainBaseManager.getAccountStore()
        .get(assetIssueCapsule.getOwnerAddress().toByteArray());

    long issuerNetUsage = issuerAccountCapsule.getNetUsage();
    long latestConsumeTime = issuerAccountCapsule.getLatestConsumeTime();
    long issuerNetLimit = calculateGlobalNetLimit(issuerAccountCapsule);

    long newIssuerNetUsage = increase(issuerNetUsage, 0, latestConsumeTime, now);

    if (bytes > (issuerNetLimit - newIssuerNetUsage)) {
      logger.debug("The " + tokenID + " issuer's bandwidth is not enough");
      return false;
    }

    //设置新的 newPublicFreeAssetNetUsage  publicLatestFreeNetTime
    latestConsumeTime = now;
    latestAssetOperationTime = now;
    publicLatestFreeNetTime = now;
    long latestOperationTime = chainBaseManager.getHeadBlockTimeStamp();

    newIssuerNetUsage = increase(newIssuerNetUsage, bytes, latestConsumeTime, now);
    newFreeAssetNetUsage = increase(newFreeAssetNetUsage,
        bytes, latestAssetOperationTime, now);
    newPublicFreeAssetNetUsage = increase(newPublicFreeAssetNetUsage, bytes,
        publicLatestFreeNetTime, now);

    issuerAccountCapsule.setNetUsage(newIssuerNetUsage);
    issuerAccountCapsule.setLatestConsumeTime(latestConsumeTime);

    assetIssueCapsule.setPublicFreeAssetNetUsage(newPublicFreeAssetNetUsage);
    assetIssueCapsule.setPublicLatestFreeNetTime(publicLatestFreeNetTime);

    accountCapsule.setLatestOperationTime(latestOperationTime);
    if (chainBaseManager.getDynamicPropertiesStore().getAllowSameTokenName() == 0) {
      accountCapsule.putLatestAssetOperationTimeMap(tokenName,
          latestAssetOperationTime);
      accountCapsule.putFreeAssetNetUsage(tokenName, newFreeAssetNetUsage);
      accountCapsule.putLatestAssetOperationTimeMapV2(tokenID,
          latestAssetOperationTime);
      accountCapsule.putFreeAssetNetUsageV2(tokenID, newFreeAssetNetUsage);

      chainBaseManager.getAssetIssueStore().put(assetIssueCapsule.createDbKey(), assetIssueCapsule);

      assetIssueCapsuleV2 =
          chainBaseManager.getAssetIssueV2Store().get(assetIssueCapsule.createDbV2Key());
      assetIssueCapsuleV2.setPublicFreeAssetNetUsage(newPublicFreeAssetNetUsage);
      assetIssueCapsuleV2.setPublicLatestFreeNetTime(publicLatestFreeNetTime);
      chainBaseManager.getAssetIssueV2Store()
          .put(assetIssueCapsuleV2.createDbV2Key(), assetIssueCapsuleV2);
    } else {
      accountCapsule.putLatestAssetOperationTimeMapV2(tokenID,
          latestAssetOperationTime);
      accountCapsule.putFreeAssetNetUsageV2(tokenID, newFreeAssetNetUsage);
      chainBaseManager.getAssetIssueV2Store()
          .put(assetIssueCapsule.createDbV2Key(), assetIssueCapsule);
    }

    chainBaseManager.getAccountStore().put(accountCapsule.createDbKey(), accountCapsule);
    chainBaseManager.getAccountStore().put(issuerAccountCapsule.createDbKey(),
        issuerAccountCapsule);

    return true;

  }

  //获取用户冻结trx获取的带宽
  public long calculateGlobalNetLimit(AccountCapsule accountCapsule) {
    //获取冻结的trx数量（带6位精度）
    long frozeBalance = accountCapsule.getAllFrozenBalanceForBandwidth();
    //冻结trx小于1 直接返回0
    if (frozeBalance < TRX_PRECISION) {
      return 0;
    }
    //冻结的trx数量
    long netWeight = frozeBalance / TRX_PRECISION;
    //获取全网总带宽（43_200_000_000L）
    long totalNetLimit = chainBaseManager.getDynamicPropertiesStore().getTotalNetLimit();
    //获取全网TRX质押量
    long totalNetWeight = chainBaseManager.getDynamicPropertiesStore().getTotalNetWeight();
    if (totalNetWeight == 0) {
      return 0;
    }
    //通过质押TRX获取的Bandwidth Point，
    //额度 = 为获取Bandwidth Point质押的TRX / 整个网络为获取Bandwidth Points质押的TRX 总额 * 43_200_000_000。
    //也就是所有用户按质押TRX平分固定额度的Bandwidth Points。
    return (long) (netWeight * ((double) totalNetLimit / totalNetWeight));
  }

  //判断转账方用户 质押代币获取的带宽是否够用
  private boolean useAccountNet(AccountCapsule accountCapsule, long bytes, long now) {
    long netUsage = accountCapsule.getNetUsage();
    long latestConsumeTime = accountCapsule.getLatestConsumeTime();
    //用户冻结trx获取的带宽
    long netLimit = calculateGlobalNetLimit(accountCapsule);

    //获取用户已使用的质押带宽
    long newNetUsage = increase(netUsage, 0, latestConsumeTime, now);

    if (bytes > (netLimit - newNetUsage)) {
      logger.debug("net usage is running out, now use free net usage");
      return false;
    }

    latestConsumeTime = now;
    long latestOperationTime = chainBaseManager.getHeadBlockTimeStamp();
    newNetUsage = increase(newNetUsage, bytes, latestConsumeTime, now);
    accountCapsule.setNetUsage(newNetUsage);
    accountCapsule.setLatestOperationTime(latestOperationTime);
    accountCapsule.setLatestConsumeTime(latestConsumeTime);

    chainBaseManager.getAccountStore().put(accountCapsule.createDbKey(), accountCapsule);
    return true;
  }

  //判断转账方用户 免费带宽是否够用
  private boolean useFreeNet(AccountCapsule accountCapsule, long bytes, long now) {

    //freeNetLimit=5000
    long freeNetLimit = chainBaseManager.getDynamicPropertiesStore().getFreeNetLimit();
    long freeNetUsage = accountCapsule.getFreeNetUsage();
    long latestConsumeFreeTime = accountCapsule.getLatestConsumeFreeTime();
    //用户已使用的 免费带宽
    long newFreeNetUsage = increase(freeNetUsage, 0, latestConsumeFreeTime, now);

    //判断用户的免费带宽是否够使用
    if (bytes > (freeNetLimit - newFreeNetUsage)) {
      logger.debug("free net usage is running out");
      return false;
    }

    long publicNetLimit = chainBaseManager.getDynamicPropertiesStore().getPublicNetLimit();
    long publicNetUsage = chainBaseManager.getDynamicPropertiesStore().getPublicNetUsage();
    long publicNetTime = chainBaseManager.getDynamicPropertiesStore().getPublicNetTime();

    long newPublicNetUsage = increase(publicNetUsage, 0, publicNetTime, now);

    //判断公共的免费带宽是否够使用
    if (bytes > (publicNetLimit - newPublicNetUsage)) {
      logger.debug("free public net usage is running out");
      return false;
    }

    latestConsumeFreeTime = now;
    long latestOperationTime = chainBaseManager.getHeadBlockTimeStamp();
    publicNetTime = now;
    newFreeNetUsage = increase(newFreeNetUsage, bytes, latestConsumeFreeTime, now);
    newPublicNetUsage = increase(newPublicNetUsage, bytes, publicNetTime, now);
    accountCapsule.setFreeNetUsage(newFreeNetUsage);
    accountCapsule.setLatestConsumeFreeTime(latestConsumeFreeTime);
    accountCapsule.setLatestOperationTime(latestOperationTime);

    chainBaseManager.getDynamicPropertiesStore().savePublicNetUsage(newPublicNetUsage);
    chainBaseManager.getDynamicPropertiesStore().savePublicNetTime(publicNetTime);
    chainBaseManager.getAccountStore().put(accountCapsule.createDbKey(), accountCapsule);
    return true;

  }

}


