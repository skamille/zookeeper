package org.apache.zookeeper;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.proto.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Encodes a composite transaction.  In the wire format, each transaction
 * consists of a single MultiHeader followed by the appropriate request.
 * Each of these MultiHeaders has a type which indicates
 * the type of the following transaction or a negative number if no more transactions
 * are included.
 */
public class MultiTransactionRecord implements Record {
    private List<Op> ops = new ArrayList<Op>();

    public MultiTransactionRecord() {
    }

    @Override
    public int hashCode() {
        return ops != null ? ops.hashCode() : 0;
    }

    public MultiTransactionRecord(Iterable<Op> ops) {
        for (Op op : ops) {
            add(op);
        }
    }

    public void add(Op op) {
        ops.add(op);
    }

    @Override
    public void serialize(OutputArchive archive, String tag) throws IOException {
        archive.startRecord(this, tag);
        for (Op op : ops) {
            MultiHeader h = new MultiHeader(op.getType());
            h.serialize(archive, tag);
            switch (op.getType()) {
                case ZooDefs.OpCode.check:
                    Op.Check check = (Op.Check) op;
                    new CheckVersionRequest(check.getPath(), check.getVersion()).serialize(archive, tag);
                    break;
                case ZooDefs.OpCode.create:
                    Op.Create create = (Op.Create) op;
                    new CreateRequest(create.getPath(), create.getData(), create.getAcl(), create.getFlags()).serialize(archive, tag);
                    break;
                case ZooDefs.OpCode.delete:
                    Op.Delete delete = (Op.Delete) op;
                    new DeleteRequest(delete.getPath(), delete.getVersion()).serialize(archive, tag);
                    break;
                case ZooDefs.OpCode.setData:
                    Op.Update update = (Op.Update) op;
                    new SetDataRequest(update.getPath(), update.getData(), update.getVersion()).serialize(archive, tag);
                    break;
                default:
                    throw new IOException("Invalid type of op");
            }
        }
        new MultiHeader(-1).serialize(archive, tag);
        archive.endRecord(this, tag);
    }

    @Override
    public void deserialize(InputArchive archive, String tag) throws IOException {
        archive.startRecord(tag);
        MultiHeader h = new MultiHeader();
        h.deserialize(archive, tag);

        while (h.getType() > 0) {
            switch (h.getType()) {
                case ZooDefs.OpCode.check:
                    CheckVersionRequest cvr = new CheckVersionRequest();
                    cvr.deserialize(archive, tag);
                    add(new Op.Check(cvr.getPath(), cvr.getVersion()));
                    break;
                case ZooDefs.OpCode.create:
                    CreateRequest cr = new CreateRequest();
                    cr.deserialize(archive, tag);
                    add(new Op.Create(cr.getPath(), cr.getData(), cr.getAcl(), cr.getFlags()));
                    break;
                case ZooDefs.OpCode.delete:
                    DeleteRequest dr = new DeleteRequest();
                    dr.deserialize(archive, tag);
                    add(new Op.Delete(dr.getPath(), dr.getVersion()));
                    break;
                case ZooDefs.OpCode.setData:
                    SetDataRequest sdr = new SetDataRequest();
                    sdr.deserialize(archive, tag);
                    add(new Op.Update(sdr.getPath(), sdr.getData(), sdr.getVersion()));
                    break;
                default:
                    throw new IOException("Invalid type of op");
            }
            h.deserialize(archive, tag);
        }
        archive.endRecord(tag);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MultiTransactionRecord)) return false;

        MultiTransactionRecord that = (MultiTransactionRecord) o;

        if (ops != null) {
            Iterator<Op> other = that.ops.iterator();
            for (Op op : ops) {
                boolean hasMoreData = other.hasNext();
                if (!hasMoreData) {
                    return false;
                }
                Op otherOp = other.next();
                if (!op.equals(otherOp)) {
                    return false;
                }
            }
            return !other.hasNext();
        } else {
            return that.ops == null;
        }

    }

}
