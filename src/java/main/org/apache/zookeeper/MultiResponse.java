package org.apache.zookeeper;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.MultiHeader;
import org.apache.zookeeper.proto.SetDataResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles the response from a multi request.  Such a response consists of
 * a sequence of responses each prefixed by a MultiResponse that indicates
 * the type of the response.  The end of the list is indicated by a MultiHeader
 * with a negative type.  Each individual response is in the same format as
 * with the corresponding operation in the original request list.
 */
public class MultiResponse implements Record {
    private List<OpResult> results;

    @Override
    public void serialize(OutputArchive archive, String tag) throws IOException {
        archive.startRecord(this, tag);
        for (OpResult result : results) {
            new MultiHeader(result.getType()).serialize(archive, tag);

            switch (result.getType()) {
                case ZooDefs.OpCode.create:
                    new CreateResponse(((OpResult.CreateResult) result).getName()).serialize(archive, tag);
                    break;
                case ZooDefs.OpCode.delete:
                    break;
                case ZooDefs.OpCode.setData:
                    new SetDataResponse(((OpResult.SetDataResult) result).getStat()).serialize(archive, tag);
                    break;
                case ZooDefs.OpCode.check:
                    new SetDataResponse(((OpResult.CheckResult) result).getStat()).serialize(archive, tag);
                    break;
                default:
            }
        }
        new MultiHeader(-1).serialize(archive, tag);
        archive.endRecord(this, tag);
    }

    @Override
    public void deserialize(InputArchive archive, String tag) throws IOException {
        results = new ArrayList<OpResult>();

        archive.startRecord(tag);
        MultiHeader h = new MultiHeader();
        h.deserialize(archive, tag);
        while (h.getType() > 0) {
            switch (h.getType()) {
                case ZooDefs.OpCode.create:
                    CreateResponse cr = new CreateResponse();
                    cr.deserialize(archive, tag);
                    results.add(new OpResult.CreateResult(cr.getPath()));
                    break;

                case ZooDefs.OpCode.delete:
                    results.add(new OpResult.DeleteResult());
                    break;

                case ZooDefs.OpCode.setData:
                    SetDataResponse sdr = new SetDataResponse();
                    sdr.deserialize(archive, tag);
                    results.add(new OpResult.SetDataResult(sdr.getStat()));
                    break;

                case ZooDefs.OpCode.check:
                    sdr = new SetDataResponse();
                    sdr.deserialize(archive, tag);
                    results.add(new OpResult.CheckResult(sdr.getStat()));
                    break;

                default:
                    throw new IOException("Invalid type " + h.getType() + " in MultiResponse");
            }
            h.deserialize(archive, tag);
        }
        archive.endRecord(tag);
    }

    public List<OpResult> getResultList() {
        return results;
    }
}
