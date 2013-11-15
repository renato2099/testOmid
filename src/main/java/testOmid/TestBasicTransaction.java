package testOmid;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.conf.Configuration;

import com.yahoo.omid.transaction.RollbackException;
import com.yahoo.omid.transaction.TTable;
import com.yahoo.omid.transaction.Transaction;
import com.yahoo.omid.transaction.TransactionException;
import com.yahoo.omid.transaction.TransactionManager;

/**
 * Testing basic transactions with Yahoo! Omid
 * @author renatomarroquin
 *
 */
public class TestBasicTransaction {

    /** HBase Configuration */
    protected static Configuration hbaseConf;

    /** Logger for the class */
    private static final Log LOG = LogFactory.getLog(TestBasicTransaction.class);

    /** Table name */
    protected static final String TEST_TABLE = "test";

    /** Family name */
    protected static final String TEST_FAMILY = "data";

    /**
     * Main program to run OMID test scenarios
     * @param args
     */
    public static void main(String[] args) {
        try {
            createHBaseConf();
            createHBaseTable();
            testSimpleTransaction();
            testRollback();
            testNeverEndingTransaction();
        } catch (IOException e) {
            LOG.error("Exception occurred", e);
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Test simple transaction scenario
     */
    public static void testSimpleTransaction() {
        String row = "test-simple";
        String col = "testdata";
        String data1 = "testWrite-1";
        String data2 = "testWrite-2";
        try {
            TransactionManager tm = new TransactionManager(hbaseConf);
            TTable tt = new TTable(hbaseConf, TEST_TABLE);

            // transaction1
            Transaction t1 = tm.begin();
            LOG.info("Transaction created " + t1);
            Put p = createPut(row, TEST_FAMILY, col, data1);
            tt.put(t1, p);
            tm.commit(t1);
            //verify
            Get g = createGet(row);
            Result r = tt.getHTable().get(g);
            String dataObtained = Bytes.toString(getValue(r, TEST_FAMILY, col));
            LOG.info(data1.equals(dataObtained)?"Tran worked as expected.":"Tran did not work as expected.");

            // transaction2
            Transaction t2 = tm.begin();
            p = createPut(row, TEST_FAMILY, col, data2);
            tt.put(t2, p);
            // transaction3
            Transaction tread = tm.begin();

            // transaction2 not committed, but showing what HBase has
            g = createGet(row);
            r = tt.getHTable().get(g);
            dataObtained = Bytes.toString(getValue(r, TEST_FAMILY, col));
            LOG.info(data2.equals(dataObtained)?"Tran worked as expected.":"Tran did not work as expected." + dataObtained);
            // transaction2 not committed, but what it sees.
            // Only sees what it has modified
            r = tt.get(t2, g);
            dataObtained = Bytes.toString(getValue(r, TEST_FAMILY, col));
            LOG.info(data1.equals(dataObtained)?"Tran worked as expected.":"Tran did not work as expected." + dataObtained);

            // trying to read uncommitted 
            r = tt.get(tread, g);
            dataObtained = Bytes.toString(getValue(r, TEST_FAMILY, col));
            LOG.info(data1.equals(dataObtained)?"Tran worked as expected.":"Tran did not work as expected." + dataObtained);
         } catch (IOException e) {
            LOG.error("Exception occurred", e);
            LOG.error(e.getMessage());
            e.printStackTrace();
         } catch (TransactionException e) {
             LOG.error("Exception occurred", e);
             LOG.error(e.getMessage());
            e.printStackTrace();
        } catch (RollbackException e) {
            LOG.error("Exception occurred", e);
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Testing transactions with rollBack option.
     */
    public static void testRollback() {
        String row = "test-simple";
        String col = "testdata";
        String data1 = "testWrite-1";
        try {
            // 1. do some processing
            TransactionManager tm = new TransactionManager(hbaseConf);
            TTable tt = new TTable(hbaseConf, TEST_TABLE);
            Transaction t1 = tm.begin();
            LOG.info("Transaction created " + t1);

            // get previous value
            Put p =  createPut(row, TEST_FAMILY, col, data1);
            tt.put(t1, p);
            Get g = createGet(row);
            Result r = tt.getHTable().get(g);

            // 2. persist
            tm.commit(t1);
            String dataObtained = Bytes.toString(getValue(r, TEST_FAMILY, col));
            LOG.info(dataObtained!=null?"Worked as expected. Not Null.":"Not worked as expected. Null.");
            // 3. rollback 
            tm.rollback(t1);
            // 4. verify
            dataObtained = Bytes.toString(getValue(r, TEST_FAMILY, col));
            LOG.info(dataObtained==null?"Worked as expected. Null.":"Not worked as expected. Not Null.");
        } catch (IOException e) {
            LOG.error("Exception occurred", e);
            LOG.error(e.getMessage());
            e.printStackTrace();
        } catch (TransactionException e) {
            LOG.error("Exception occurred", e);
            LOG.error(e.getMessage());
            e.printStackTrace();
        } catch (RollbackException e) {
            LOG.error("Exception occurred", e);
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Testing NeverEndingTransaction
     */
    public static void testNeverEndingTransaction() {
        // commit is never called and no rollback is called 
        // check what data is
        String row = "test-simple";
        String col = "testdata";
        String data6 = "testWrite-6";
        try {
            TransactionManager tm = new TransactionManager(hbaseConf);
            TTable tt = new TTable(hbaseConf, TEST_TABLE);
            Transaction t1 = tm.begin();
            Transaction t2 = tm.begin();
            LOG.info("Transaction created " + t1);
            LOG.info("Transaction created " + t2);

            // get previous value
            Get g = createGet(row);
            tt.get(t1, g);
            Result r = tt.get(t1, g);
            String previousData = Bytes.toString(getValue(r, TEST_FAMILY, col));

            // putting uncommitted data
            Put p = createPut(row, TEST_FAMILY, col, data6);
            tt.put(t2, p);
            // reading before committing from transaction 1
            g = createGet(row);
            r = tt.get(t1, g);
            String dataObtained = Bytes.toString(getValue(r, TEST_FAMILY, col));
            LOG.info(dataObtained.equals(previousData)?"Worked as expected.":"Not worked as expected.");
            // transaction3
            Transaction t3 = tm.begin();
            LOG.info("Transaction created " + t3);
            // reading before committing from transaction 3
            r = tt.get(t3, g);
            dataObtained = Bytes.toString(getValue(r, TEST_FAMILY, col));
            LOG.info(dataObtained.equals(previousData)?"Worked as expected.":"Not worked as expected.");

            // after committing
            tm.commit(t2);
            // reading after committing from transaction3
            r = tt.get(t3, g);
            dataObtained = Bytes.toString(getValue(r, TEST_FAMILY, col));
            // Note: t3 can't see changes committed by t2 because started before t2 was committed
            LOG.info(dataObtained.equals(data6)?"Worked as expected.":"Not worked as expected.");
            // transaction4
            Transaction t4 = tm.begin();
            LOG.info("Transaction created " + t4);
            r = tt.get(t4, g);
            dataObtained = Bytes.toString(getValue(r, TEST_FAMILY, col));
            // Note: t2 can see changes before it started after changes were committed by t2
            LOG.info(dataObtained.equals(data6)?"Worked as expected.":"Not worked as expected.");
           } catch (IOException e) {
               LOG.error("Exception occurred", e);
               LOG.error(e.getMessage());
               e.printStackTrace();
           } catch (TransactionException e) {
               LOG.error("Exception occurred", e);
               LOG.error(e.getMessage());
               e.printStackTrace();
           } catch (RollbackException e) {
               LOG.error("Exception occurred", e);
               LOG.error(e.getMessage());
               e.printStackTrace();
        } 
    }

    /**
     * Creates a HBaseConf using default parameters
     * @throws IOException
     */
    public static void createHBaseConf() throws IOException {
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.coprocessor.region.classes", 
                 "com.yahoo.omid.regionserver.Compacter");
        hbaseConf.setInt("hbase.hregion.memstore.flush.size", 100*1024);
        hbaseConf.setInt("hbase.regionserver.nbreservationblocks", 1);
        hbaseConf.set("tso.host", "localhost");
        hbaseConf.setInt("tso.port", 1234);
        final String rootdir = "/tmp/hbase.test.dir/";
        File rootdirFile = new File(rootdir);
        if (rootdirFile.exists()) {
           delete(rootdirFile);
        }
        hbaseConf.set("hbase.rootdir", rootdir);
    }

    /**
     * Creates a put for a specific row
     * @param pRow Name
     * @param pFam Name
     * @param pCol Name
     * @param pData to be put
     * @return HBase Put
     */
    private static Put createPut(String pRow, String pFam, String pCol, String pData) {
        byte[] row = Bytes.toBytes(pRow);
        byte[] fam = Bytes.toBytes(pFam);
        byte[] col = Bytes.toBytes(pCol);
        byte[] data1 = Bytes.toBytes(pData);
        Put p = new Put(row);
        p.add(fam, col, data1);
        return p;
    }

    /**
     * Creates a get for a specific row
     * @param pRow Name
     * @return
     */
    private static Get createGet(String pRow){
        byte[] row = Bytes.toBytes(pRow);
        Get g = null;
        try {
             g = new Get(row).setMaxVersions(1);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } 
        return g;
    }

    /**
     * Gets a value from a result of a specific family and column
     * @param pRes Result
     * @param pFam Family
     * @param pCol Column
     * @return Byte array containing the value
     */
    private static byte[] getValue(Result pRes, String pFam, String pCol) {
        byte[] fam = Bytes.toBytes(pFam);
        byte[] col = Bytes.toBytes(pCol);
        return pRes.getValue(fam, col);
    }

    /**
     * Creates an HBase table if it doesn't exist
     * @throws IOException
     */
    private static void createHBaseTable() throws IOException { 
        HBaseAdmin admin = new HBaseAdmin(hbaseConf);

        if (!admin.tableExists(TEST_TABLE)) {
           HTableDescriptor desc = new HTableDescriptor(TEST_TABLE);
           HColumnDescriptor datafam = new HColumnDescriptor(TEST_FAMILY);
           datafam.setMaxVersions(Integer.MAX_VALUE);
           desc.addFamily(datafam);
           admin.createTable(desc);
        }

        if (admin.isTableDisabled(TEST_TABLE)) {
           admin.enableTable(TEST_TABLE);
        }
        HTableDescriptor[] tables = admin.listTables();
        for (HTableDescriptor t : tables) {
           LOG.info(t.getNameAsString());
        }
        admin.close();
    }

    /**
     * Deletes the temporary file
     * @param f
     * @throws IOException
     */
    private static void delete(File f) throws IOException {
        if (f.isDirectory()) {
          for (File c : f.listFiles())
            delete(c);
        }
        if (!f.delete())
          throw new FileNotFoundException("Failed to delete file: " + f);
    }
}
