package cn.bithachi.demo.socket;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @Author: BitHachi
 * @Email: bithachi@163.com
 * @Date: 2022/8/26
 * @Description:
 */
public class BlackListSocket {
    static ServerSocket serverSocket = null;
    static PrintWriter pw = null;

    public static void main(String[] args) {
        try {
            serverSocket = new ServerSocket(9999);
            System.out.println("服务启动，等待连接");
            Socket socket = serverSocket.accept();
            System.out.println("连接成功，来自：" + socket.getRemoteSocketAddress());
            pw = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
            int j = 0;
            while (j < 100) {
                String str = null;
                j++;
                if (j % 2 == 0) {
                    str = System.currentTimeMillis() + " " + "tom";
                } else if (j % 3 == 0) {
                    str = System.currentTimeMillis() + " " + "leo";
                } else {
                    str = System.currentTimeMillis() + " " + "USER-" + j;
                }
                pw.println(str);
                pw.flush();
                System.out.println(str);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                pw.close();
                serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}