package borg.ed.elasticupdate.eddb;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;

import borg.ed.galaxy.util.MiscUtil;

import java.lang.reflect.Type;

/**
 * BooleanDigitDeserializer
 *
 * @author <a href="mailto:b.guenther@xsite.de">Boris Guenther</a>
 */
public class BooleanDigitDeserializer implements JsonDeserializer<Boolean> {

    @Override
    public Boolean deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        if (json.isJsonNull()) {
            return null;
        } else if (json.isJsonPrimitive()) {
            JsonPrimitive jsonPrimitive = json.getAsJsonPrimitive();
            if (jsonPrimitive.isBoolean()) {
                return jsonPrimitive.getAsBoolean();
            } else if (jsonPrimitive.isNumber()) {
                return jsonPrimitive.getAsInt() != 0;
            }
        }
        return MiscUtil.getAsBoolean(json.getAsString());
    }

}
