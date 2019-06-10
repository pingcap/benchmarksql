public class CommitException extends RuntimeException {

    private static final long serialVersionUID = 2135244094396431474L;

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
