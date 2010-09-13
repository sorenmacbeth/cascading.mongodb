package cascading.mongodb;

import cascading.ClusterTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.mongodb.document.DefaultMongoDocument;
import cascading.mongodb.document.GameDocument;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;

import java.io.IOException;
import java.util.Properties;

/**
 * Date: May 25, 2010
 * Time: 12:28:27 AM
 */
public class MongoDBTest extends ClusterTestCase {

    String inputFile = "src/test/data/testdata.txt";

    static final String HOST = "arrow.mongohq.com";
    static final int PORT = 27081;
    static final String COLLECTION = "cascadingtest";
    static final String DB = "gameattain-staging";
    static final String USER = "gameattain";
    static final char[] PASS = { 'G', 'A', '.', '2', '0', '0', '9'};

    public MongoDBTest() {
        super("mongodb tap test", false);
    }

    @Override public void setUp() throws IOException {
        super.setUp();
    }

    @Override
    public void tearDown() throws IOException {
    }


    public void testMongoDBWrites() throws Exception {

        //Create new document from source data.
        Pipe parsePipe = new Pipe("insert");

        Fields tupleFields = new Fields("letter", "number", "symbol");
        Fields selector = new Fields("letter", "number");

        Tap source = new Lfs(new TextLine(), inputFile);
        parsePipe = new Each(parsePipe, new Fields("line"), new RegexSplitter(tupleFields, "\\s"));

        Tap mongoTap = new MongoDBTap(HOST, PORT, DB, COLLECTION, new MongoDBScheme(MongoDBOutputFormat.class, MongoDBInputFormat.class), new DefaultMongoDocument(selector));

        Properties props = new Properties();
        Flow parseFlow = new FlowConnector(props).connect(source, mongoTap, parsePipe);

        parseFlow.complete();

    }

    public void testMongoDBReads() throws Exception
    {
        Pipe newPipe = new Pipe("read");
        Fields tupleFields = new Fields("letter", "number", "symbol");
        Fields selector = new Fields("letter", "number");

        Tap source = new MongoDBTap(HOST, PORT, DB, COLLECTION, new MongoDBScheme(null, MongoDBInputFormat.class), new DefaultMongoDocument(selector));
        newPipe = new Each(newPipe, Fields.ALL, new Identity());

        Tap sink = new Lfs(new TextLine(), "file:///tmp/reads.txt");

        Properties props = new Properties();
        Flow readFlow = new FlowConnector(props).connect(source, sink, newPipe);

        readFlow.complete();
    }

    public void testRemoteMongoDBReads() throws Exception
    {
        Pipe newPipe = new Pipe("read");
        Fields tupleFields = new Fields("letter", "number", "symbol");
        Fields selector = new Fields("letter", "number");

        Tap source = new MongoDBTap(HOST, PORT, DB, COLLECTION, USER, PASS, new MongoDBScheme(MongoDBInputFormat.class), new DefaultMongoDocument(selector));
        newPipe = new Each(newPipe, Fields.ALL, new Identity());

        Tap sink = new Lfs(new TextLine(), "file:///tmp/reads.txt");

        Properties props = new Properties();
        Flow readFlow = new FlowConnector(props).connect(source, sink, newPipe);

        readFlow.complete();
    }

    public void testRemoteGameDocumentReads() throws Exception
    {
        Pipe pipe = new Pipe("Read Games");
        Fields outputSelector = new Fields("title", "article");

        GameDocument queryDocument = new GameDocument(outputSelector);
        Tap source = new MongoDBTap(HOST, PORT, DB, "games", USER, PASS, new MongoDBScheme(null, MongoDBInputFormat.class), queryDocument);

        pipe = new Each(pipe, Fields.ALL, new Identity(new Fields("title", "article")));

        Tap sink = new Lfs(new TextLine(), "file:///tmp/training");

        Properties props = new Properties();
        Flow readFlow = new FlowConnector(props).connect(source, sink, pipe);

        readFlow.complete();
    }


    private void verifySink(Flow flow, int expects) throws IOException {
        int count = 0;

        TupleEntryIterator iterator = flow.openSink();

        while (iterator.hasNext()) {
            count++;
            System.out.println("iterator.next() = " + iterator.next());
        }

        iterator.close();

        assertEquals("wrong number of values", expects, count);
    }
}
