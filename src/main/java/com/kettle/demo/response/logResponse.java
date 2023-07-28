package com.kettle.demo.response;

import lombok.Data;

import java.util.List;

@Data
public class logResponse {
    List<String> list;
    Long pointer;
}
