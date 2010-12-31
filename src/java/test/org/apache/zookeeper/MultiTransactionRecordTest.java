package org.apache.zookeeper;

import junit.framework.TestCase;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class MultiTransactionRecordTest extends TestCase {
    @Test
    public void testRoundTrip() throws IOException {
        MultiTransactionRecord request = new MultiTransactionRecord();
        request.add(new Op.Check("check", 1));
        request.add(new Op.Create("create", "create data".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, ZooDefs.Perms.ALL));
        request.add(new Op.Delete("delete", 17));
        request.add(new Op.SetData("setData", "set data".getBytes(), 19));

        MultiTransactionRecord decodedRequest = codeDecode(request);

        assertEquals(request, decodedRequest);
        assertEquals(request.hashCode(), decodedRequest.hashCode());
    }

    @Test
    public void testEmptyRoundTrip() throws IOException {
        MultiTransactionRecord request = new MultiTransactionRecord();
        MultiTransactionRecord decodedRequest = codeDecode(request);

        assertEquals(request, decodedRequest);
        assertEquals(request.hashCode(), decodedRequest.hashCode());
    }

    private MultiTransactionRecord codeDecode(MultiTransactionRecord request) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
        request.serialize(boa, "request");
        baos.close();
        ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
        bb.rewind();

        BinaryInputArchive bia = BinaryInputArchive.getArchive(new ByteBufferInputStream(bb));
        MultiTransactionRecord decodedRequest = new MultiTransactionRecord();
        decodedRequest.deserialize(bia, "request");
        return decodedRequest;
    }
}
