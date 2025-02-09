package io.openmessaging.benchmark.pojo.output;

import lombok.Data;

@Data
public class TestDetails {
    public String uuid;
    public String testStartTime;
    public long testRunDurationInMinutes;
    public String product;
    public String sku;
    public String protocol;
    public Metadata metadata;
}
