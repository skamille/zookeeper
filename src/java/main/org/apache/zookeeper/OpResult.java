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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof OpResult)) return false;

            CreateResult other = (CreateResult) o;
            return getType() == other.getType() && name.equals(other.name);
        }

        @Override
        public int hashCode() {
            return getType() * 35 + name.hashCode();
        }
    }

    public static class DeleteResult extends OpResult {
        public DeleteResult() {
            super(ZooDefs.OpCode.delete);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof OpResult)) return false;

            OpResult opResult = (OpResult) o;
            return getType() == opResult.getType();
        }

        @Override
        public int hashCode() {
            return getType();
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof OpResult)) return false;

            SetDataResult other = (SetDataResult) o;
            return getType() == other.getType() && stat.getMzxid() == other.stat.getMzxid();
        }

        @Override
        public int hashCode() {
            return (int) (getType() * 35 + stat.getMzxid());
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof OpResult)) return false;

            CheckResult other = (CheckResult) o;
            return getType() == other.getType() && stat.getMzxid() == other.stat.getMzxid();
        }

        @Override
        public int hashCode() {
            return (int) (getType() * 35 + stat.getMzxid());
        }
    }
}
