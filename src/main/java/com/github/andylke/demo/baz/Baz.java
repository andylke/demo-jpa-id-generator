package com.github.andylke.demo.baz;

import java.io.Serializable;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;

import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.Parameter;

@Entity
@Table(name = "baz")
@IdClass(BazId.class)
public class Baz implements Serializable {

  private static final long serialVersionUID = 1L;

  @Id
  @GeneratedValue(generator = "baz_seq")
  @GenericGenerator(
      name = "baz_seq",
      strategy = "com.github.andylke.demo.TableGeneratorSegmentByFields",
      parameters = {
        @Parameter(name = "table_name", value = "baz_seq"),
        @Parameter(name = "segment_column_names", value = "code, language_code"),
        @Parameter(name = "segment_column_sizes", value = "10, 2"),
        @Parameter(name = "segment_value_field_names", value = "code, languageCode"),
        @Parameter(name = "value_column_name", value = "next_id")
      })
  private int id;

  @Id private String code;

  @Id private String languageCode;

  private String text;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public String getLanguageCode() {
    return languageCode;
  }

  public void setLanguageCode(String languageCode) {
    this.languageCode = languageCode;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }
}
