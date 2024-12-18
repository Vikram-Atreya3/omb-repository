package microsoft.azure.eventhub.perftesting.framework.pojo.inputs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.text.MessageFormat;
import lombok.Getter;
import lombok.ToString;
import microsoft.azure.eventhub.perftesting.framework.utils.payload.PayloadException;
import microsoft.azure.eventhub.perftesting.framework.utils.payload.PayloadReader;

@Getter
@ToString
public class Payload {

  public int payloadSize;
  public String payloadFile;
  @ToString.Exclude @JsonIgnore byte[] payloadData;

  public Payload(int payloadSize, String payloadFileName, PayloadReader payloadReader) {
    this.payloadSize = payloadSize;
    this.payloadFile = payloadFileName;

    payloadData = payloadReader.load(this.payloadFile);
  }

  public void validate() throws IllegalArgumentException {
    if (payloadSize != payloadData.length) {
      throw new PayloadException(
          MessageFormat.format(
              "Payload length mismatch. Actual is: {0}, but expected: {1} ",
              payloadData.length, payloadSize));
    }
  }
}
