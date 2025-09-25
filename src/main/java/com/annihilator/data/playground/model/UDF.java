package com.annihilator.data.playground.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class UDF {

    private String id;
    private String userId;
    private String name;
    private String functionName;
    private String jarS3Path;
    private String className;
    private String parameterTypes;
    private String returnType;
    private String description;
    private long createdAt;
}



