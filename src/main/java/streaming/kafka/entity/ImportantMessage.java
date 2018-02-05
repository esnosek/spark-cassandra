package streaming.kafka.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ImportantMessage extends Message {

    public ImportantMessage(String id, String text){
        this.id = id;
        this.text = text;
    }

    private String id;
    private String text;
    private String type = "IMPORTANT";
}
