package org.apache.zookeeper;

import junit.framework.TestCase;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class MultiResponseTest extends TestCase {
    public void testRoundTrip() throws IOException {
        MultiResponse response = new MultiResponse();

        Stat s = new Stat();
        s.setCzxid(134);
        response.add(new OpResult.CheckResult(s));
        response.add(new OpResult.CreateResult("foo-bar"));
        response.add(new OpResult.DeleteResult());

        s.setCzxid(546);
        response.add(new OpResult.SetDataResult(s));

        MultiResponse decodedResponse = codeDecode(response);

        assertEquals(response, decodedResponse);
        assertEquals(response.hashCode(), decodedResponse.hashCode());
    }

    @Test
    public void testEmptyRoundTrip() throws IOException {
        MultiResponse result = new MultiResponse();
        MultiResponse decodedResult = codeDecode(result);

        assertEquals(result, decodedResult);
        assertEquals(result.hashCode(), decodedResult.hashCode());
    }

    private MultiResponse codeDecode(MultiResponse request) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
        request.serialize(boa, "result");
        baos.close();
        ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
        bb.rewind();

        BinaryInputArchive bia = BinaryInputArchive.getArchive(new ByteBufferInputStream(bb));
        MultiResponse decodedRequest = new MultiResponse();
        decodedRequest.deserialize(bia, "result");
        return decodedRequest;
    }

}
