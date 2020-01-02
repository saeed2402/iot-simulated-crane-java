// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// This application uses the Azure IoT Hub device SDK for Java
// For samples see: https://github.com/Azure/azure-iot-sdk-java/tree/master/device/iot-device-samples

package com.microsoft.docs.iothub.samples;

import com.microsoft.azure.sdk.iot.device.*;
import com.microsoft.azure.sdk.iot.device.DeviceTwin.*;
import com.google.gson.Gson;

import java.io.*;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.time.format.DateTimeFormatter;  
import java.time.LocalDateTime;

public class SimulatedDevice {
  // The device connection string to authenticate the device with your IoT hub.
  // Using the Azure CLI:
  // az iot hub device-identity show-connection-string --hub-name {YourIoTHubName} --device-id MyJavaDevice --output table
  private static String connString = "HostName=test-direct-method-scala-01.azure-devices.net;DeviceId=MyJavaDevice-01;SharedAccessKey=vyKha/0D0IGv5FIygKKbuMbyyRdPp7EVQgJuU8u5rrQ=";

  // Using the MQTT protocol to connect to IoT Hub
  private static IotHubClientProtocol protocol = IotHubClientProtocol.MQTT;
  private static DeviceClient client;

  // Define method response codes
  private static final int METHOD_SUCCESS = 200;
  private static final int METHOD_NOT_DEFINED = 404;
  private static final int INVALID_PARAMETER = 400;

  private static int telemetryInterval = 1000;
  private static double heightIncrements = 0.5;

  // Specify the telemetry to send to your IoT hub.
  private static class TelemetryDataPoint {
    public String device_id;
    public double temperature;
    public double humidity;
    public double height;
    public LocalDateTime device_time;
    public double latitude;
    public double longitude;
    public double wind_speed;
    public double load_weight;
    public double lift_angle;

    // Serialize object to JSON format.
    public String serialize() {
      Gson gson = new Gson();
      return gson.toJson(this);
    }
  }

  // Print the acknowledgement received from IoT Hub for the method acknowledgement sent.
  protected static class DirectMethodStatusCallback implements IotHubEventCallback
  {
    public void execute(IotHubStatusCode status, Object context)
    {
      System.out.println("Direct method # IoT Hub responded to device method acknowledgement with status: " + status.name());
    }
  }

  // Print the acknowledgement received from IoT Hub for the telemetry message sent.
  private static class EventCallback implements IotHubEventCallback {
    public void execute(IotHubStatusCode status, Object context) {
      System.out.println("IoT Hub responded to message with status: " + status.name());

      if (context != null) {
        synchronized (context) {
          context.notify();
        }
      }
    }
  }

  protected static class DirectMethodCallback implements DeviceMethodCallback
  {
    private static DecimalFormat df2 = new DecimalFormat("#.###");
    private void setTelemetryInterval(int interval)
    {
      System.out.println("Direct method # Setting telemetry interval (seconds): " + interval);
      telemetryInterval = interval * 1000;
    }

    private void setHeightIncrements(double increments)
    {
      double hinc = Double.parseDouble(df2.format(increments));
      System.out.println("Setting height increments to: " + hinc);
      heightIncrements = hinc;
    }

    @Override
    public DeviceMethodData call(String methodName, Object methodData, Object context)
    {
      DeviceMethodData deviceMethodData;
      String payload = new String((byte[])methodData);
      final DecimalFormat df2 = new DecimalFormat("#.##");
      
      switch (methodName)
      {
        case "SetTelemetryInterval" :
        {
          int interval;
          try {
            int status = METHOD_SUCCESS;
            interval = Integer.parseInt(payload);
            System.out.println("---------> "+payload + " , "+interval);
            setTelemetryInterval(interval);
            deviceMethodData = new DeviceMethodData(status, "Executed direct method " + methodName);
          } catch (NumberFormatException e) {
            int status = INVALID_PARAMETER;
            deviceMethodData = new DeviceMethodData(status, "Invalid parameter " + payload);
          }
          break;
        }

        case "setHeightIncrements" :
        {
          double increment;
          try {
            int status = METHOD_SUCCESS;
            
            //String p = payload.substring(0, 3);
            //System.out.println("Adjustment Percentage: "+p);
            double pp = (1 - (Double.parseDouble(payload)/100));
            increment = pp * heightIncrements;
            //System.out.println("increment: "+increment+",pp: "+pp+",heightIncrements: "+heightIncrements);
            System.out.println("Direct method -> slowing crane by: "+ df2.format(Double.parseDouble(payload))+"%");
            setHeightIncrements(increment );
            deviceMethodData = new DeviceMethodData(status, "Executed direct method " + methodName);
          } catch (NumberFormatException e) {
            int status = INVALID_PARAMETER;
            deviceMethodData = new DeviceMethodData(status, "Invalid parameter " + payload);
          }
          break;
        }


        default:
        {
          int status = METHOD_NOT_DEFINED;
          deviceMethodData = new DeviceMethodData(status, "Not defined direct method " + methodName);
        }
      }
      return deviceMethodData;
    }
  }

  private static class MessageSender implements Runnable {
    private static DecimalFormat df2 = new DecimalFormat("#.###");
    //double dd;
    public void run() {
      try {
        // Initialize the simulated telemetry.
        double minTemperature = 2.0;
        double minHumidity = 60;
        double currentHeight = 13.0;
        Random rand = new Random();
        String deviceID = "MyJavaDevice-01";
        double latitude = -37.816368;
        double longitude = 144.967005;
        double currentTemperature;
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");  
        LocalDateTime now = LocalDateTime.now();  
        double loadWeight = 1.5;
        //double minWindSpeed = 13.0;
        double currentWindSpeed = 2.0;

        while (true) {
          // Simulate telemetry.
          
          //double currentTemperature = 34.1;//minTemperature + rand.nextDouble() * 15;
          double currentHumidity = minHumidity + rand.nextDouble() * 20;
          //dd=currentHeight + heightIncrements;
          currentHeight = Double.parseDouble(df2.format(currentHeight + heightIncrements));
          currentWindSpeed = currentWindSpeed + rand.nextDouble() * 0.1;
          currentTemperature = minTemperature + rand.nextDouble() * 2;
          //System.out.println(currentHeight +"..."+ heightIncrements);
          now = LocalDateTime.now(); 
          
          TelemetryDataPoint telemetryDataPoint = new TelemetryDataPoint();
          telemetryDataPoint.temperature = currentTemperature;
          telemetryDataPoint.humidity = currentHumidity;
          telemetryDataPoint.height = currentHeight;
          telemetryDataPoint.latitude = latitude;
          telemetryDataPoint.longitude = longitude;
          telemetryDataPoint.device_id = deviceID;
          telemetryDataPoint.device_time = now;
          telemetryDataPoint.load_weight = loadWeight;
          telemetryDataPoint.wind_speed = currentWindSpeed;
          telemetryDataPoint.lift_angle = 2.1042;


          // Add the telemetry to the message body as JSON.
          String msgStr = telemetryDataPoint.serialize();
          Message msg = new Message(msgStr);

          // Add a custom application property to the message.
          // An IoT hub can filter on these properties without access to the message body.
          msg.setProperty("temperatureAlert", (currentTemperature > 30) ? "true" : "false");
          
          System.out.println("Sending message: " + msgStr);

          Object lockobj = new Object();

          // Send the message.
          EventCallback callback = new EventCallback();
          client.sendEventAsync(msg, callback, lockobj);

          synchronized (lockobj) {
            lockobj.wait();
          }
          Thread.sleep(telemetryInterval);
        }
      } catch (InterruptedException e) {
        System.out.println("Finished.");
      }
    }
  }

  public static void main(String[] args) throws IOException, URISyntaxException {

    // Connect to the IoT hub.
    client = new DeviceClient(connString, protocol);
    client.open();

    // Register to receive direct method calls.
    client.subscribeToDeviceMethod(new DirectMethodCallback(), null, new DirectMethodStatusCallback(), null);
    
    // Create new thread and start sending messages 
    MessageSender sender = new MessageSender();
    ExecutorService executor = Executors.newFixedThreadPool(1);
    executor.execute(sender);

    // Stop the application.
    System.out.println("Press ENTER to exit.");
    System.in.read();
    executor.shutdownNow();
    client.closeNow();
  }
}
