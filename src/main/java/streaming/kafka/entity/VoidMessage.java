package streaming.kafka.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class VoidMessage extends Message {

    public VoidMessage(String id, String text){
        this.id = id;
        this.text = text;
    }

    private String id;
    private String text;
    private String type = "VOID";
}
