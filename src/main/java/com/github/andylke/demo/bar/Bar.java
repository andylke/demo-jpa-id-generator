package com.github.andylke.demo.bar;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.TableGenerator;

@Entity
@Table(name = "bar")
public class Bar {

  @Id
  @GeneratedValue(strategy = GenerationType.TABLE, generator = "bar_seq")
  @TableGenerator(
      name = "bar_seq",
      table = "bar_seq",
      pkColumnName = "name",
      valueColumnName = "next_id",
      allocationSize = 1)
  private int id;

  private String text;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }
}
