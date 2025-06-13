import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Main {

    private static final byte[] messageSize = new byte[4];
    private static final byte[] correlationId = new byte[4];
    private static final byte[] requestApiKey = new byte[2];
    private static final byte[] requestApiVersion = new byte[2];

  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.err.println("Logs from your program will appear here!");

     ServerSocket serverSocket = null;
     Socket clientSocket = null;
     int port = 9092;
     try {
       serverSocket = new ServerSocket(port);
       // Since the tester restarts your program quite often, setting SO_REUSEADDR
       // ensures that we don't run into 'Address already in use' errors
       serverSocket.setReuseAddress(true);
       // Wait for connection from client.
       clientSocket = serverSocket.accept();

         DataInputStream in = new DataInputStream(clientSocket.getInputStream());
         byte[] inputBytes = new byte[1024];
         in.readFully(inputBytes, 0, 12);

         updateHeaders(messageSize, correlationId, requestApiVersion, requestApiKey, inputBytes);

         DataOutputStream outputStream =
                 new DataOutputStream(clientSocket.getOutputStream());

         outputStream.writeInt(byteArrayToInt(messageSize, ByteOrder.BIG_ENDIAN));
         outputStream.writeInt(byteArrayToInt(correlationId, ByteOrder.BIG_ENDIAN));
         outputStream.writeShort(35);

     } catch (IOException e) {
       System.out.println("IOException: " + e.getMessage());
     } finally {
       try {
         if (clientSocket != null) {
           clientSocket.close();
         }
       } catch (IOException e) {
         System.out.println("IOException: " + e.getMessage());
       }
     }
  }

    private static void updateHeaders(byte[] messageSize, byte[] correlationId, byte[] requestApiVersion, byte[] requestApiKey, byte[] inputBytes) {
      ByteBuffer byteBuffer = ByteBuffer.wrap(inputBytes);
      byteBuffer.get(0, messageSize, 0,  4);
      byteBuffer.get(4, requestApiKey, 0,  2);
      byteBuffer.get(6, requestApiVersion, 0,  2);
      byteBuffer.get(8, correlationId, 0,  4);

  }

    public static int byteArrayToInt(byte[] bytes, ByteOrder order) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(order);
        return buffer.getInt();
    }
}
