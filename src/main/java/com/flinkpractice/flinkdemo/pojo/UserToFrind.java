package com.flinkpractice.flinkdemo.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class UserToFrind {
    private String uid;
    private String gender;
    private String name;
    private int fid;
    private String fname;
}
