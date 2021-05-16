package com.kavinduraisamy91.demo.model;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class Post {
	
	private String name;
	private Boolean like;
	private Boolean disLike;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Boolean getLike() {
		return like;
	}
	public void setLike(Boolean like) {
		this.like = like;
	}
	public Boolean getDisLike() {
		return disLike;
	}
	public void setDisLike(Boolean disLike) {
		this.disLike = disLike;
	}
	public Post(String name, Boolean like, Boolean disLike) {
		super();
		this.name = name;
		this.like = like;
		this.disLike = disLike;
	}
	
	public Post() {
		
	}
	

}
