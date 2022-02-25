package org.tron.test;

import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.tron.common.utils.ByteArray;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

/**
 * @author kiven.miao
 * @date 2022/2/22 3:56 下午
 */
public class DBConvertEngine {
  private String srcDir;
  private String dbName;
  private Path srcDbPath;

  private int srcDbKeyCount = 0;
  private int dstDbKeyCount = 0;
  private int srcDbKeySum = 0;
  private int dstDbKeySum = 0;
  private int srcDbValueSum = 0;
  private int dstDbValueSum = 0;

  public DBConvertEngine(String src, String name) {
    this.srcDir = src;
    this.dbName = name;
    this.srcDbPath = Paths.get(this.srcDir, name);
  }

  public static void main(String[] args) {
    String dbSrc = "/Users/lanyu/Code/gitCode/java-tron/output-directory/database";
    String readSrc = "properties";
    File dbDirectory = new File(dbSrc);
    if (!dbDirectory.exists()) {
      System.out.println(dbSrc + "does not exist.");
      return;
    }
    File[] files = dbDirectory.listFiles();

    if (files == null || files.length == 0) {
      System.out.println(dbSrc + "does not contain any database.");
      return;
    }

    long time = System.currentTimeMillis();
    for (File file : files) {
      if(!readSrc.equals(file.getName())){
        continue;
      }
      if (!file.isDirectory()) {
        System.out.println(file.getName() + " is not a database directory, ignore it.");
        continue;
      }
      try {
        DBConvertEngine convert = new DBConvertEngine(dbSrc, file.getName());
        if (convert.doConvert()) {
          System.out.println(String
              .format(
                  "Convert database %s successful with %s key-value. keySum: %d, valueSum: %d",
                  convert.dbName,
                  convert.srcDbKeyCount, convert.dstDbKeySum, convert.dstDbValueSum));
        } else {
          System.out.println(String.format("Convert database %s failure", convert.dbName));
        }
      } catch (Exception e) {
        System.out.println(e.getMessage());
        return;
      }
    }
    System.out.println(String
        .format("database convert use %d seconds total.",
            (System.currentTimeMillis() - time) / 1000));
  }


  private static org.iq80.leveldb.Options newDefaultLevelDbOptions() {
    org.iq80.leveldb.Options dbOptions = new org.iq80.leveldb.Options();
    dbOptions.createIfMissing(true);
    dbOptions.paranoidChecks(true);
    dbOptions.verifyChecksums(true);
    dbOptions.compressionType(CompressionType.SNAPPY);
    dbOptions.blockSize(4 * 1024);
    dbOptions.writeBufferSize(10 * 1024 * 1024);
    dbOptions.cacheSize(10 * 1024 * 1024L);
    dbOptions.maxOpenFiles(100);
    return dbOptions;
  }

  private boolean doConvert() {

    File levelDbFile = srcDbPath.toFile();
    if (!levelDbFile.exists()) {
      System.out.println(srcDbPath.toString() + "does not exist.");
      return false;
    }

    DB level = null;
    try {
      level = newLevelDb(srcDbPath);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return convertLevelDbData(level);
  }

  private boolean convertLevelDbData(DB level){
    // convert
    DBIterator levelIterator = level.iterator();
    try {
      for (levelIterator.seekToFirst(); levelIterator.hasNext(); levelIterator.next()) {
        byte[] key = levelIterator.peekNext().getKey();
        byte[] value = levelIterator.peekNext().getValue();
        srcDbKeyCount++;
        srcDbKeySum = byteArrayToIntWithOne(srcDbKeySum, key);
        srcDbValueSum = byteArrayToIntWithOne(srcDbValueSum, value);
        System.out.println("key : " + new String(key) + "======  value : " + ByteArray.toLong(value));
      }
      return true;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    } finally {
      try {
        levelIterator.close();
      } catch (IOException e1) {
        e1.printStackTrace();
      }
    }
  }


  private DB newLevelDb(Path db) throws IOException {
    DB database;
    File file = db.toFile();
    org.iq80.leveldb.Options dbOptions = newDefaultLevelDbOptions();
    try {
      database = factory.open(file, dbOptions);
    } catch (IOException e) {
      if (e.getMessage().contains("Corruption:")) {
        factory.repair(file, dbOptions);
        database = factory.open(file, dbOptions);
      } else {
        throw e;
      }
    }
    return database;
  }

  private int byteArrayToIntWithOne(int sum, byte[] b) {
    for (byte oneByte : b) {
      sum += oneByte;
    }
    return sum;
  }

}
