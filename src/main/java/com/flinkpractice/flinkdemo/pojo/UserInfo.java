package com.flinkpractice.flinkdemo.pojo;

import lombok.Data;

import java.util.List;

@Data
public class UserInfo {
    private String uid;
    private String gender;
    private String name;
    private List<FriendInfo> friends;
}
