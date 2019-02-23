package kt.cli;

class OptionValidationException extends RuntimeException {
    OptionValidationException(String message) {
        super(message);
    }
}
