package org.tron.test;

/**
 * @author kiven.miao
 * @date 2022/2/23 5:45 下午
 */
public class ResourceTest {

  private static long precision =  1000000L;
  private static long windowSize = 24 * 3600 * 1000L/1000L;


  public static void main(String[] args) {
    ResourceTest resourceTest = new ResourceTest();

    long lastUsage = 0L;
    long usage = 0L;
    long lastTime = 0L;
    long now = 1645549200L;

    System.out.println("=======result : " + resourceTest.increase(lastUsage, usage, lastTime, now));

  }

  private long increase(long lastUsage, long usage, long lastTime, long now) {
    return increase(lastUsage, usage, lastTime, now, windowSize);
  }

  /**
   * 计算用户获取的免费带宽
   * @param lastUsage 上次交易使用带宽
   * @param usage 本次使用带宽
   * @param lastTime 上次交易时间
   * @param now 当前时间
   * @param windowSize 时间窗口 24 * 3600 * 1000L/3000L
   * @return 用户获取的免费带宽
   */
  private long increase(long lastUsage, long usage, long lastTime, long now, long windowSize) {
    long averageLastUsage = divideCeil(lastUsage * precision, windowSize);
    long averageUsage = divideCeil(usage * precision, windowSize);

    if (lastTime != now) {
      assert now > lastTime;
      if (lastTime + windowSize > now) {
        long delta = now - lastTime;
        double decay = (windowSize - delta) / (double) windowSize;
        averageLastUsage = Math.round(averageLastUsage * decay);
      } else {
        averageLastUsage = 0;
      }
    }
    averageLastUsage += averageUsage;
    return getUsage(averageLastUsage, windowSize);
  }

  private long divideCeil(long numerator, long denominator) {
    return (numerator / denominator) + ((numerator % denominator) > 0 ? 1 : 0);
  }

  private long getUsage(long usage, long windowSize) {
    return usage * windowSize / precision;
  }

}
