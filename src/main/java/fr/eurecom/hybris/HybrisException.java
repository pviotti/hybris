package fr.eurecom.hybris;

public class HybrisException extends Exception {

    private static final long serialVersionUID = 1L;

    public HybrisException(String message){
        super(message);
    }

    public HybrisException(String message, Throwable t){
        super(message, t);
    }

    public HybrisException(Exception e) {
        super(e);
    }
}
