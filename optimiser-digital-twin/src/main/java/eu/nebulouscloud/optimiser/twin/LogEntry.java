package eu.nebulouscloud.optimiser.twin;

record LogEntry(
    String CompName,
    String ReplicaID,
    String RemoteCompName,
    LogEntryType EventType,
    String ActivityID,
    long EventTime,
    long PayloadSize)
{
    public enum LogEntryType {
        IN,
        OUT,
        ACK,
        UNKNOWN;
        public static LogEntryType fromString(String t) {
            return switch(t) {
                case "in" -> IN;
                case "out" -> OUT;
                case "ack" -> ACK;
                default -> UNKNOWN;
            };
        }
        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }
}
