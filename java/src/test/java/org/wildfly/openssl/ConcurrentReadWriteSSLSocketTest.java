package org.wildfly.openssl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Pedro Ruivo
 * @since 2.1.5
 */
public class ConcurrentReadWriteSSLSocketTest extends AbstractOpenSSLTest {

   private static void close(Closeable closeable) {
      try {
         closeable.close();
      } catch (IOException e) {
         //ignore
      }
   }

   @Test
   public void testTLS1_2() throws Exception {
      doTest("TLSv1.2");
   }

   @Test
   public void testTLS1_3() throws Exception {
      doTest("TLSv1.3");
   }

   private void doTest(String protocol) throws IOException, InterruptedException {
      try (ServerSocket serverSocket = createServerSocket(protocol)) {
         Thread acceptThread = new EchoServer(protocol, serverSocket);
         acceptThread.start();
         final SSLContext sslContext = SSLTestUtils.createClientSSLContext("openssl." + protocol);
         try (final SSLSocket socket = (SSLSocket) sslContext.getSocketFactory().createSocket()) {
            socket.setReuseAddress(true);
            socket.connect(SSLTestUtils.createSocketAddress());
            // the workaround is to do the handshake before starting the receiver thread
            // socket.startHandshake();
            Receiver receiver = new Receiver(protocol, socket);
            receiver.start();

            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            out.writeUTF("hello world");
            out.flush();

            Assert.assertEquals("hello world", receiver.getData());

            // all good
            receiver.interrupt();
            acceptThread.interrupt();

            socket.close();
            serverSocket.close();
            acceptThread.join();
            receiver.join();
         }
      }
   }

   private ServerSocket createServerSocket(String protocol) throws IOException {
      SSLContext context = SSLTestUtils.createSSLContext(protocol);
      ServerSocket socket = context.getServerSocketFactory().createServerSocket(SSLTestUtils.PORT);
      socket.setReuseAddress(true);
      return socket;
   }

   private static class Receiver extends Thread {

      private final BlockingDeque<String> data;
      private final SSLSocket socket;
      private volatile boolean stop;

      private Receiver(String protocol, SSLSocket socket) {
         super("concurrent-test-receiver-" + protocol);
         this.socket = socket;
         this.data = new LinkedBlockingDeque<>();
         this.stop = false;
      }

      @Override
      public void run() {
         try {
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            while (!stop) {
               data.add(in.readUTF());
            }
         } catch (IOException e) {
            e.printStackTrace();
         }
      }

      @Override
      public void interrupt() {
         stop = true;
         super.interrupt();
      }

      private String getData() throws InterruptedException {
         return data.poll(5000, TimeUnit.MILLISECONDS);
      }
   }

   private static class EchoServer extends Thread {

      private final ServerSocket serverSocket;
      private final List<EchoServerThread> connections;
      private volatile boolean stop;

      private EchoServer(String protocol, ServerSocket serverSocket) {
         super("concurrent-test-acceptor-" + protocol);
         this.serverSocket = serverSocket;
         connections = new LinkedList<>();
      }

      @Override
      public void run() {
         try {
            while (!stop) {
               Socket socket = serverSocket.accept();
               EchoServerThread t = new EchoServerThread(socket);
               t.start();
               connections.add(t);
            }
         } catch (IOException e) {
            e.printStackTrace();
         } finally {
            connections.forEach(EchoServerThread::interrupt);
            connections.clear();
         }
      }

      @Override
      public void interrupt() {
         stop = true;
         super.interrupt();
      }
   }

   private static class EchoServerThread extends Thread {

      private final Socket socket;
      private volatile boolean stop;

      private EchoServerThread(Socket socket) {
         super("concurrent-test-server-thread-" + socket.getRemoteSocketAddress());
         this.socket = socket;
      }

      @Override
      public void run() {
         try {
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            while (!stop) {
               String data = in.readUTF();
               out.writeUTF(data);
               out.flush();
            }
         } catch (IOException e) {
            e.printStackTrace();
         } finally {
            close(socket);
         }
      }

      @Override
      public void interrupt() {
         stop = true;
         super.interrupt();
      }
   }

}
