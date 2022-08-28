package cn.bithachi.demo.streaming;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/28
 * @Description:
 */
public class MyReceiver extends Receiver  {
    private String host;
    private int port;

    MyReceiver(String host, int port) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public MyReceiver(StorageLevel storageLevel) {
        super(StorageLevel.MEMORY_AND_DISK_2());
    }

    /**
     * Receiver 启动时调用
     */
    @Override
    public void onStart() {
        // 启动通过Socket连接接收数据的线程
        new Thread(() -> receive()).start();
    }

    /**
     * Receiver 停止时调用
     */
    @Override
    public void onStop() {
        //如果isStopped()返回false,那么调用receive ()方法的线程将自动停止
        //因此此处无须做太多工作
    }

    /**
     * 创建Socket连接并接收数据，直到Receiver停止
     */
    private void receive() {
        Socket socket = null;
        String userInput = null;
        try {
            // 连接host:port
            socket = new Socket(host, port);
            // 读取数据
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            userInput = reader.readLine();
            while (!isStopped() && userInput != null) {
                // 存储数据到内存中
                store(userInput);
                userInput = reader.readLine();
            }
            reader.close();
            socket.close();

            // 重新启动，以便在服务器再次激活时重新连接
            restart("Trying to connect again");

        } catch (Exception e) {
            if (e.getClass().equals(ConnectException.class)) {
                // 如果无法连接服务器，就重新启动
                restart("Error connecting to host: " + host + " port: " + port, e);
            }
            // 如果有其它任何错误，就重新启动
            restart("Error  receiving data", e);
        }
    }
}
