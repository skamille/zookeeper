package org.apache.zookeeper;

import org.apache.zookeeper.data.Stat;

/**
 * Encodes the result of a single part of a multiple operation commit.
 */
public class OpResult {
    private int type;

    private OpResult(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public static class CreateResult extends OpResult {
        private String name;

        public CreateResult(String name) {
            super(ZooDefs.OpCode.create);
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public static class DeleteResult extends OpResult {
        public DeleteResult() {
            super(ZooDefs.OpCode.delete);
        }
    }

    public static class SetDataResult extends OpResult {
        private Stat stat;

        public SetDataResult(Stat stat) {
            super(ZooDefs.OpCode.setData);
            this.stat = stat;
        }

        public Stat getStat() {
            return stat;
        }
    }

    public static class CheckResult extends OpResult {
        private Stat stat;

        public CheckResult(Stat stat) {
            super(ZooDefs.OpCode.check);
            this.stat = stat;
        }

        public Stat getStat() {
            return stat;
        }
    }
}
