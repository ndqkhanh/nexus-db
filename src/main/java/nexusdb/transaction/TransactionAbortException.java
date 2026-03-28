package nexusdb.transaction;

/**
 * Thrown when a transaction cannot commit due to a conflict or validation failure.
 */
public class TransactionAbortException extends RuntimeException {

    private final long txnId;
    private final String reason;

    public TransactionAbortException(long txnId, String reason) {
        super("Transaction " + txnId + " aborted: " + reason);
        this.txnId = txnId;
        this.reason = reason;
    }

    public long txnId() { return txnId; }
    public String reason() { return reason; }
}
