package org.Ardor.StaticDataDownloader;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Downloader
{
    public static void main( String[] args )
    {
        final String JDBC_URL = "jdbc:postgresql://localhost:5433/ardorImages";
        final String USERNAME = "postgres";
        final String PASSWORD = "1234";
        final String QUERY = "SELECT NAME FROM M_AIRLINE";
        final String ENDPOINT = "https://images.trippro.com/AirlineImages/AirLine/GDS/images/";
        final int THREADS = 10;
        final String PARENT_FOLDER ="C:\\Downloaded_Images\\AirlineImages_gif";
        final String COLUMN_NAME = "NAME";
        final String EXTENSION = ".gif";

        //Creating Threads
        ExecutorService executorService = Executors.newFixedThreadPool(THREADS);

        //Creating List to Store Airline Names
        List<String> airlineNames = new ArrayList<>();

        //Connecting to Database and retrieving the data
        try(Connection connection = DriverManager.getConnection(JDBC_URL,USERNAME,PASSWORD))
        {
            PreparedStatement preparedStatement = connection.prepareStatement(QUERY);
            ResultSet resultSet = preparedStatement.executeQuery();
            while(resultSet.next())
            {
                //Finds the column name and stores in the List
                airlineNames.add(resultSet.getString(COLUMN_NAME).replace(" ",""));
            }
        }catch (SQLException sqlException)
        {
            System.err.println(sqlException.getMessage());
        }

        for(String name : airlineNames)
        {
            final String FINAL_URL = ENDPOINT + name + EXTENSION;
            final String foldername = PARENT_FOLDER + File.separator + name;
            executorService.submit( ()->
                    {
                        gifDownloader(FINAL_URL,foldername + File.separator + name + EXTENSION, foldername);
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        executorService.shutdown();
    }
    private static void gifDownloader(String url,String filePath,String folderName)
    {
        HttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url);
        try
        {
            HttpResponse httpResponse = httpClient.execute(httpGet);
            int httpStatus = httpResponse.getStatusLine().getStatusCode();

            if(httpStatus == 200)
            {
                File folder = new File(folderName);
                folder.mkdirs();

                try(InputStream inputStream = httpResponse.getEntity().getContent();
                    FileOutputStream fileOutputStream = new FileOutputStream(filePath)) {

                    byte[] buffer = new byte[4096];
                    int bytesRead;

                    while((bytesRead = inputStream.read(buffer)) != -1)
                    {
                        fileOutputStream.write(buffer,0,bytesRead);
                    }
                    System.out.println("Download Successful " + filePath);
                }
            }else
            {
                FileWriter fileWriter = new FileWriter("C:\\Downloaded_Images\\FailedLogs_gif",true);
                System.out.println("Unable to Download : " + filePath);
                fileWriter.write(url +"  Failed to Download\n");
                fileWriter.close();
            }
        }catch(IOException e)
        {
            System.err.println("Exception Connection to Client");
        }
    }
}
