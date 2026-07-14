public enum HttpStatus {

    OK(200, "OK"),
    MOVED_PERMANENTLY(301, "Moved Permanently"),
    FOUND(302, "Found"),

    BAD_REQUEST(400, "Bad Request"),
    UNAUTHORIZED(401, "Unauthorized"),
    FORBIDDEN(403, "Forbidden"),
    NOT_FOUND(404, "Not Found"),

    INTERNAL_SERVER_ERROR(500, "Internal Server Error"),
    NOT_IMPLEMENTED(501, "Not Implemented"),
    BAD_GATEWAY(502, "Bad Gateway"),

    UNKNOWN(-1, "Unknown Status");

    private final int code;
    private final String description;

    HttpStatus(int code, String description) {
        this.code = code;
        this.description = description;
    }

    public static HttpStatus getStatusFromCode(int code) {
        for (HttpStatus status : HttpStatus.values()) {
            if (status.code == code) {
                return status;
            }
        }
        return UNKNOWN;
    }

    public String getDescription() {
        return description;
    }
}