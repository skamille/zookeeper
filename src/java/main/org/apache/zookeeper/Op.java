package org.apache.zookeeper;

import org.apache.zookeeper.data.ACL;

import java.util.List;

/**
 * Represents a single operation in a multi-operation transaction.  Each operation can be a create, update
 * or delete or can just be a version check.
 *
 * Sub-classes of Op each represent each detailed type.
 */
class Op {
    private int type;

    // prevent bare construction
    private Op(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public static class Create extends Op {
        private String path;
        private byte[] data;
        private List<ACL> acl;
        private int flags;

        public Create(String path, byte[] data, List<ACL> acl, int flags) {
            super(ZooDefs.OpCode.create);
            this.path = path;
            this.data = data;
            this.acl = acl;
            this.flags = flags;
        }

        public String getPath() {
            return path;
        }

        public byte[] getData() {
            return data;
        }

        public List<ACL> getAcl() {
            return acl;
        }

        public int getFlags() {
            return flags;
        }
    }

    public static class Delete extends Op {
        private String path;
        private int version;

        public Delete(String path, int version) {
            super(ZooDefs.OpCode.delete);
            this.path = path;
            this.version = version;
        }

        public String getPath() {
            return path;
        }

        public int getVersion() {
            return version;
        }
    }

    public static class Update extends Op {
        private String path;
        private byte[] data;
        private int version;

        public Update(String path, byte[] data, int version) {
            super(ZooDefs.OpCode.setData);
            this.path = path;
            this.data = data;
            this.version = version;
        }

        public String getPath() {
            return path;
        }

        public byte[] getData() {
            return data;
        }

        public int getVersion() {
            return version;
        }
    }

    public static class Check extends Op {
        private String path;
        private int version;

        public Check(String path, int version) {
            super(ZooDefs.OpCode.check);
            this.path = path;
            this.version = version;
        }

        public String getPath() {
            return path;
        }

        public int getVersion() {
            return version;
        }
    }
}
