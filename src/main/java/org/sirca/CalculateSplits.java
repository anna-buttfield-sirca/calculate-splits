package org.sirca;

import me.prettyprint.cassandra.connection.client.HThriftClient;
import me.prettyprint.cassandra.service.CassandraHost;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class CalculateSplits {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.print("Usage: CalculateSplits host keyspace column_family_name");
            System.exit(1);
        }
        String hostName = args[0];
        String keyspace = args[1];
        String columnFamilyName = args[2];
        System.out.println("Running describe_splits with host: " + hostName + ", keyspace: " + keyspace + ", column family " + columnFamilyName);

        CassandraHost host = new CassandraHost(hostName);
        HThriftClient client = new HThriftClient(host);
        client.open();
        long t = System.currentTimeMillis();
        try {
            List<String> splits = client.getCassandra(keyspace).describe_splits(columnFamilyName, "0", "0", 32000);

            System.out.println("number of splits: " + splits.size());
            System.out.println("time (ms): " + (System.currentTimeMillis() - t));

            PrintWriter writer = new PrintWriter("splits.txt", "UTF-8");
            for (String split : splits) {
                writer.println(split);
            }
            writer.close();
            System.out.println("Splits listed in splits.txt");

        } catch (InvalidRequestException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
}
