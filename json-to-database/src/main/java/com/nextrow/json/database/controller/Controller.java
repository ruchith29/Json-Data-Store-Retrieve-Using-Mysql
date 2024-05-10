package com.nextrow.json.database.controller;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;
import java.util.*;

import java.sql.*;

@RestController
public class Controller {

    @Autowired
    private ResourceLoader resourceLoader;

    // to load the database connection from app.properties file
    @Autowired
    private Environment env;

    @GetMapping("/storeData")
    public void storeData() throws IOException {

        int count=0;
        int ExecutedCount=0;

        // to read the json file
        Resource resource = resourceLoader.getResource("classpath:sample.json");
        InputStreamReader inputStreamReader = new InputStreamReader(resource.getInputStream());
        String text = FileCopyUtils.copyToString(inputStreamReader);

        // converting string to json object
        JSONObject jsonObject = new JSONObject(text);
        JSONObject componentObject = jsonObject.getJSONObject("components");

        try
        {
            // to store the data that is not stored in database due to some exceptions
            File file=new File("ExceptionData.txt");
            Writer writer=new FileWriter(file);

            // to get jdbc connection
            String url = env.getProperty("spring.datasource.url");
            String username = env.getProperty("spring.datasource.username");
            String password = env.getProperty("spring.datasource.password");

            // to establish database connection
            Connection con = DriverManager.getConnection(url, username, password);
            Statement st = con.createStatement();

            // iterating over table object
            for (String s : componentObject.keySet()) {
            JSONArray jsonArray = componentObject.getJSONArray(s);

            // changing the name of table since we cannot create table with special characters
            String tableName=s.replace('|', '_').replace('-', '_');

            // appending table name to file
            writer.append("The following are Exception raised values due to insufficient columns or values: ").append(tableName);
            writer.append("\n");

            // iterating over array and the values are stored as objects
            for (int i = 0; i < jsonArray.length(); i++) {

                    JSONObject object = jsonArray.getJSONObject(i);
                    StringBuffer stringBuffer=new StringBuffer();

                    // iterating over object to get the array values and keys
                    for (String key : object.keySet()) {

                        Object value = object.get(key);// storing the values
                        key=key.replace(' ','_');// removing spaces to avoid exceptions

                        // removing extra char if length is >64 sql can't add the data as column name
                        if (key.length()>64){
                            key=key.substring(0,64);
                        }
                        stringBuffer.append("`").append(key).append("` VARCHAR(255), ");// used back quotes for creating col name with special char

                    }

                    // removing the extra chars that are added to buffer
                    stringBuffer.setLength(stringBuffer.length()-2);
                    // converting buffer to string
                    String string=(stringBuffer).toString();

                    // query to create the table
                    String query="CREATE TABLE IF NOT EXISTS "+tableName+" ("+string+")";

                    // execute the query
                    st.execute(query);

                    // print statement to tell user that tables are created
                    System.out.println("Table: "+s+" is created");
                    // used break since everytime we can't create the same table
                    break;
                }

/* --------------------------for fetching values from database--------------------------*/

                // iterating over array and the values are stored as objects
                for (int i = 0; i < jsonArray.length(); i++) {

                    JSONObject object = jsonArray.getJSONObject(i);

                    // used buffer to store the key and values from the object
                    StringBuffer stringBuffer=new StringBuffer();
                    StringBuffer columns=new StringBuffer();

                    HashMap<String,Object> keyAndValue=new HashMap<>();

                    // iterating over object to get the array values and keys
                    for (String key : object.keySet()) {

                        String Key = key.replace(' ', '_').substring(0, Math.min(key.length(), 64));
                        Object value = object.get(key);
                        String Value = value.toString().replace("'", "''").substring(0, Math.min(value.toString().length(), 255));

                        columns.append(Key).append(",");
                        stringBuffer.append("'").append(Value).append("',");

                        keyAndValue.put(key,Value);
                    }

                    // removing extra chars and convert to string
                    columns.setLength(columns.length()-1);
                    String col=columns.toString();

                    // removing extra chars and convert to string
                    stringBuffer.setLength(stringBuffer.length()-1);
                    String values=stringBuffer.toString();

                    // query to insert values to table
                    String qry="INSERT INTO "+tableName+" VALUES "+"("+values+")";

                  try {
                      ExecutedCount++;// to store the number of values that are successfully stored in database
                      st.execute(qry);
                      }
                  catch (Exception e){
                      for (String k:keyAndValue.keySet()){

                          // adding the data that is not stored in database due to some exception to an external file
                          writer.append(k);
                          writer.append(" : ");
                          String objectToString=keyAndValue.get(k).toString();
                          writer.append(objectToString);
                          writer.append("\n");
                      }
                      writer.append("\n");
                      count++;// to store the number of values that are not successfully stored in database
                    }

                }
                writer.append("\n");
                System.out.println("Insertion completed");
            }

        }
        catch (Exception e){
            System.out.println("Caught Exception "+e);
        }

        System.out.println("ExecutedCount: "+ExecutedCount);
        System.out.println("Error count: "+count);
    }


    @GetMapping("/returnData")
    public HashMap<String,HashMap<String,Object>> returnData() throws IOException, SQLException {
        Resource resource = resourceLoader.getResource("classpath:sample.json");
        InputStreamReader inputStreamReader = new InputStreamReader(resource.getInputStream());
        String text = FileCopyUtils.copyToString(inputStreamReader);

        JSONObject jsonObject = new JSONObject(text);
        JSONObject componentObject = jsonObject.getJSONObject("components");

        HashMap<String,HashMap<String,Object>> totalData=new HashMap<>();
        try {
            String url = env.getProperty("spring.datasource.url");
            String username = env.getProperty("spring.datasource.username");
            String password = env.getProperty("spring.datasource.password");

            // Establish database connection
            Connection con = DriverManager.getConnection(url, username, password);
            Statement st = con.createStatement();
            DatabaseMetaData databaseMetaData = con.getMetaData();

            HashMap<String,Object> hashMap=new HashMap<>();

            // to store data of rows and columns in form of object
            ArrayList<Object> arrayList=new ArrayList<>();

            for (String s : componentObject.keySet()) {

                JSONArray jsonArray=componentObject.getJSONArray(s);
                String Name=s.replace('|', '_').replace('-', '_');
                String tableName = Name.toLowerCase();

                // getting the table data from db
                ResultSet resultSet = st.executeQuery("SELECT * FROM "+tableName);

                // used metadata to get col count and fetch col names
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();

                //used to store the column name and corresponding fields in form of keys and values
                HashMap<String,Object> keyValue=new HashMap<>();

                while (resultSet.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        keyValue.put(metaData.getColumnLabel(i),resultSet.getObject(i));
                    }
                    arrayList.add(keyValue);
                    }
                // to store the table name and table data
                hashMap.put(s,arrayList);

            }

            // returning the table data with db name in json format to user
            totalData.put("components",hashMap);
            return totalData;
        }
        catch(Exception e)
        {
            System.out.println("Exception: "+e);
        }
        return null;
    }
}