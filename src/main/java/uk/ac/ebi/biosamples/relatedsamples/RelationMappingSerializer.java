package uk.ac.ebi.biosamples.relatedsamples;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by lucacherubin on 2016/09/22.
 */
public class RelationMappingSerializer extends JsonDeserializer<Map<String,String>> {

    @Value("${relations.unwanted}")
    private List<String> unwantedRelations;
    public void serialize(Map<String, String> relations, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode object = mapper.createObjectNode();

//        Map<String, String> filteredRelations = new HashMap<>();
//        filteredRelations.put("accession", (String) relations.get("accession"));
//        Map<String, Map<String, String>> links = (Map<String, Map<String, String>>) relations.get("_links");
//        links.entrySet().forEach(el ->  {
//            if (! unwantedRelations.contains(el.getKey())) {
//                Map<String, String> linkValue = el.getValue();
//                filteredRelations.put(el.getKey(), linkValue.get("href"));
//            }
//        });
        jsonGenerator.writeObject(object);
    }

    @Override
    public Map<String, String> deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
        return null;
    }
}
