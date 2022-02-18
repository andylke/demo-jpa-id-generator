package com.github.andylke.demo.baz;

import java.io.Serializable;
import java.math.BigDecimal;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.Parameter;

@Entity
@Table(name = "baz")
public class Baz implements Serializable {

  private static final long serialVersionUID = 1L;

  @Id
  @GeneratedValue(generator = "baz_seq")
  @GenericGenerator(
      name = "baz_seq",
      strategy = "com.github.andylke.demo.TableGeneratorSegmentByFields",
      parameters = {
        @Parameter(name = "table_name", value = "baz_seq"),
        @Parameter(name = "segment_column_names", value = "code,language_code"),
        @Parameter(name = "segment_column_sizes", value = "3,3"),
        @Parameter(name = "segment_value_field_names", value = "code,languageCode"),
        @Parameter(name = "value_column_name", value = "next_id"),
        @Parameter(name = "concat_segment_and_sequence", value = "true"),
        @Parameter(name = "left_pad_segment_and_sequence_char", value = "0"),
        @Parameter(name = "left_pad_segment_and_sequence_sizes", value = "3,3,3"),
        @Parameter(name = "select_for_update_retry_attempts", value = "2")
      })
  private BigDecimal id;

  private BigDecimal code;

  private BigDecimal languageCode;

  private String text;

  public BigDecimal getId() {
    return id;
  }

  public void setId(BigDecimal id) {
    this.id = id;
  }

  public BigDecimal getCode() {
    return code;
  }

  public void setCode(BigDecimal code) {
    this.code = code;
  }

  public BigDecimal getLanguageCode() {
    return languageCode;
  }

  public void setLanguageCode(BigDecimal languageCode) {
    this.languageCode = languageCode;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }
}
