package Nifi;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class NifiControllerService {
    private String id;
    private String state;
    private int version;

    public void incrementVersion() {
        version++;
    }
}
