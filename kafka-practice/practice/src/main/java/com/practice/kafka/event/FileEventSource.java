package com.practice.kafka.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutionException;

public class FileEventSource implements Runnable{
    public static final Logger logger = LoggerFactory.getLogger(FileEventSource.class.getName());

    boolean keepRunning = true;
    private int updateInterval;
    private File file;
    private long filePointer = 0;
    private EventHandler eventHandler;

    public FileEventSource(int updateInterval, File file, EventHandler eventHandler) {
        this.updateInterval = updateInterval;
        this.file = file;
        this.eventHandler = eventHandler;
    }

    // 주기적으로 변화가 있으면 실행하는 코드
    @Override
    public void run() {
        try {
            while (this.keepRunning) {
                Thread.sleep(this.updateInterval);
                // file의 크기를 계산
                long len = this.file.length();
                //
                if (len < this.filePointer) {
                    logger.info("log file was reset as filePointer is longer than file length");
                    filePointer = len;
                    // file 크기가 커지면 = 파일이 변하면
                } else if (len > this.filePointer) {
                    readAppendAndSend();
                } else {
                    continue;
                }
            }
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        } catch (ExecutionException e) {
            logger.error(e.getMessage());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    private void readAppendAndSend() throws IOException, ExecutionException, InterruptedException {
        RandomAccessFile raf = new RandomAccessFile(this.file, "r");
        raf.seek(this.filePointer);
        String line = null;
        while ((line = raf.readLine()) != null) {
            sendMessage(line);
        }
        //file이 qusrudehldjTdmamfh 랴ㅣㄷdml 랴ㅣ뎨ㅐㅑㅕㅜㅅㄷㄱfmf guswo file의 마지막으로 재 설정함
        this.filePointer = raf.getFilePointer();
    }

    private void sendMessage(String line) {
    }
}
