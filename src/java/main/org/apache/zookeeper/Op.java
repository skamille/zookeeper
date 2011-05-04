/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper;

import org.apache.jute.Record;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.SetACLRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.data.ACL;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Represents a single operation in a multi-operation transaction.  Each operation can be a create, update
 * or delete or can just be a version check.
 *
 * Sub-classes of Op each represent each detailed type.
 */
public abstract class Op {
    private int type;

    // prevent untyped construction
    private Op(int type) {
        this.type = type;
    }

    public static Op create(String path, byte[] data, List<ACL> acl, int flags) {
        return new Create(path, data, acl, flags);
    }

    public static Op create(String path, byte[] data, List<ACL> acl, CreateMode createMode) {
        return new Create(path, data, acl, createMode);
    }

    public static Op delete(String path, int version) {
        return new Delete(path, version);
    }

    public static Op setData(String path, byte[] data, int version) {
        return new SetData(path, data, version);
    }

    public static Op check(String path, int version) {
        return new Check(path, version);
    }

    public int getType() {
        return type;
    }

    public abstract Record toRequestRecord() ;

    public static class Create extends Op {
        private String path;
        private byte[] data;
        private List<ACL> acl;
        private int flags;

        private Create(String path, byte[] data, List<ACL> acl, int flags) {
            super(ZooDefs.OpCode.create);
            this.path = path;
            this.data = data;
            this.acl = acl;
            this.flags = flags;
        }

        private Create(String path, byte[] data, List<ACL> acl, CreateMode createMode) {
            super(ZooDefs.OpCode.create);
            this.path = path;
            this.data = data;
            this.acl = acl;
            this.flags = createMode.toFlag();
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Create)) return false;

            Create op = (Create) o;

            boolean aclEquals = true;
            Iterator<ACL> i = op.getAcl().iterator();
            for (ACL acl : getAcl()) {
                boolean hasMoreData = i.hasNext();
                if (!hasMoreData) {
                    aclEquals = false;
                    break;
                }
                ACL otherAcl = i.next();
                if (!acl.equals(otherAcl)) {
                    aclEquals = false;
                    break;
                }
            }
            return !i.hasNext() && getType() == op.getType() && Arrays.equals(data, op.data) && flags == op.flags && aclEquals;
        }

        @Override
        public int hashCode() {
            return super.getType() + path.hashCode() + Arrays.hashCode(data);
        }

        @Override
        public Record toRequestRecord() {
            return new CreateRequest(path, data, acl, flags);
        }
    }

    public static class Delete extends Op {
        private String path;
        private int version;

        private Delete(String path, int version) {
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Delete)) return false;

            Delete op = (Delete) o;

            return getType() == op.getType() && version == op.version && path.equals(op.path);
        }

        @Override
        public int hashCode() {
            return super.getType() + path.hashCode() + version;
        }

        @Override
        public Record toRequestRecord() {
            return new DeleteRequest(path, version);
        }
    }

    public static class SetData extends Op {
        private String path;
        private byte[] data;
        private int version;

        private SetData(String path, byte[] data, int version) {
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SetData)) return false;

            SetData op = (SetData) o;

            return getType() == op.getType() && version == op.version && path.equals(op.path) && Arrays.equals(data, op.data);
        }

        @Override
        public int hashCode() {
            return super.getType() + path.hashCode() + Arrays.hashCode(data) + version;
        }

        @Override
        public Record toRequestRecord() {
            return new SetDataRequest(path, getData(), version);
        }
    }

    public static class Check extends Op {
        private String path;
        private int version;

        private Check(String path, int version) {
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Check)) return false;

            Check op = (Check) o;

            return getType() == op.getType() && path.equals(op.path) && version == op.version;
        }

        @Override
        public int hashCode() {
            return super.getType() + path.hashCode() + version;
        }

        @Override
        public Record toRequestRecord() {
            return new CheckVersionRequest(path, version);
        }
    }
}
