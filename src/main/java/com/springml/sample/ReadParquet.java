package com.springml.sample;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;


public class ReadParquet {

  static void runReadParquet() {
    Pipeline p = Pipeline.create();

    Schema schema =
            new Schema.Parser()
                    .parse("{\n" +
                            "  \"type\": \"record\",\n" +
                            "  \"name\": \"schema\",\n" +
                            "  \"fields\": [\n" +
                            "    {\n" +
                            "      \"name\": \"StudentId\",\n" +
                            "      \"type\": [\n" +
                            "        \"null\",\n" +
                            "        \"long\"\n" +
                            "      ]\n" +
                            "    },\n" +
                            "    {\n" +
                            "      \"name\": \"CourseTitle\",\n" +
                            "      \"type\": [\n" +
                            "        \"null\",\n" +
                            "        \"string\"\n" +
                            "      ]\n" +
                            "    },\n" +
                            "    {\n" +
                            "      \"name\": \"CourseCode\",\n" +
                            "      \"type\": [\n" +
                            "        \"null\",\n" +
                            "        \"string\"\n" +
                            "      ]\n" +
                            "    },\n" +
                            "    {\n" +
                            "      \"name\": \"MaximumSeats\",\n" +
                            "      \"type\": [\n" +
                            "        \"null\",\n" +
                            "        \"long\"\n" +
                            "      ]\n" +
                            "    },\n" +
                            "    {\n" +
                            "      \"name\": \"CourseStartDate\",\n" +
                            "      \"type\": [\n" +
                            "        \"null\",\n" +
                            "        \"string\"\n" +
                            "      ]\n" +
                            "    },\n" +
                            "    {\n" +
                            "      \"name\": \"CourseEndDate\",\n" +
                            "      \"type\": [\n" +
                            "        \"null\",\n" +
                            "        \"string\"\n" +
                            "      ]\n" +
                            "    },\n" +
                            "    {\n" +
                            "      \"name\": \"PlacementDate\",\n" +
                            "      \"type\": [\n" +
                            "        \"null\",\n" +
                            "        \"string\"\n" +
                            "      ]\n" +
                            "    },\n" +
                            "    {\n" +
                            "      \"name\": \"ExitDate\",\n" +
                            "      \"type\": [\n" +
                            "        \"null\",\n" +
                            "        \"string\"\n" +
                            "      ]\n" +
                            "    },\n" +
                            "    {\n" +
                            "      \"name\": \"Location\",\n" +
                            "      \"type\": [\n" +
                            "        \"null\",\n" +
                            "        \"string\"\n" +
                            "      ]\n" +
                            "    },\n" +
                            "    {\n" +
                            "      \"name\": \"Building\",\n" +
                            "      \"type\": [\n" +
                            "        \"null\",\n" +
                            "        \"string\"\n" +
                            "      ]\n" +
                            "    },\n" +
                            "    {\n" +
                            "      \"name\": \"Room\",\n" +
                            "      \"type\": [\n" +
                            "        \"null\",\n" +
                            "        \"string\"\n" +
                            "      ]\n" +
                            "    },\n" +
                            "    {\n" +
                            "      \"name\": \"Semester\",\n" +
                            "      \"type\": [\n" +
                            "        \"null\",\n" +
                            "        \"string\"\n" +
                            "      ]\n" +
                            "    },\n" +
                            "    {\n" +
                            "      \"name\": \"Credits\",\n" +
                            "      \"type\": [\n" +
                            "        \"null\",\n" +
                            "        \"long\"\n" +
                            "      ]\n" +
                            "    },\n" +
                            "    {\n" +
                            "      \"name\": \"ClassFormat\",\n" +
                            "      \"type\": [\n" +
                            "        \"null\",\n" +
                            "        \"string\"\n" +
                            "      ]\n" +
                            "    },\n" +
                            "    {\n" +
                            "      \"name\": \"SessionWeeks\",\n" +
                            "      \"type\": [\n" +
                            "        \"null\",\n" +
                            "        \"string\"\n" +
                            "      ]\n" +
                            "    },\n" +
                            "    {\n" +
                            "      \"name\": \"WeekDay\",\n" +
                            "      \"type\": [\n" +
                            "        \"null\",\n" +
                            "        \"string\"\n" +
                            "      ]\n" +
                            "    },\n" +
                            "    {\n" +
                            "      \"name\": \"StartTime\",\n" +
                            "      \"type\": [\n" +
                            "        \"null\",\n" +
                            "        \"string\"\n" +
                            "      ]\n" +
                            "    },\n" +
                            "    {\n" +
                            "      \"name\": \"EndTime\",\n" +
                            "      \"type\": [\n" +
                            "        \"null\",\n" +
                            "        \"string\"\n" +
                            "      ]\n" +
                            "    },\n" +
                            "    {\n" +
                            "      \"name\": \"Hours\",\n" +
                            "      \"type\": [\n" +
                            "        \"null\",\n" +
                            "        \"long\"\n" +
                            "      ]\n" +
                            "    }\n" +
                            "  ]\n" +
                            "}");

    PCollection<GenericRecord> records = p.apply(ParquetIO.read(schema).from("src/main/resources/parquet/ADECourse.parquet"));
    PCollection < Long > count = records.apply(Count.globally());
    count.apply(ParDo.of(new DoFn<Long, Void>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        c.element();
      }
    }));

    PCollection<TableRow> rows = records.apply(ParDo.of(new DoFn<GenericRecord, TableRow>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        TableRow row = new TableRow();

        for(Schema.Field field : c.element().getSchema().getFields()) {
          String fieldName = field.name();
          row.set(fieldName, c.element().get(fieldName));
        }

        c.output(row);

    }}));

    TableSchema tableSchemaJson =
            new TableSchema()
                    .setFields(
                            Arrays.asList(
                                    new TableFieldSchema()
                                            .setName("StudentId")
                                            .setType("NUMERIC"),
                                    new TableFieldSchema()
                                            .setName("CourseTitle")
                                            .setType("STRING"),
                                    new TableFieldSchema()
                                            .setName("CourseCode")
                                            .setType("STRING"),
                                    new TableFieldSchema()
                                            .setName("MaximumSeats")
                                            .setType("NUMERIC"),
                                    new TableFieldSchema()
                                            .setName("CourseStartDate")
                                            .setType("STRING"),
                                    new TableFieldSchema()
                                            .setName("CourseEndDate")
                                            .setType("STRING"),
                                    new TableFieldSchema()
                                            .setName("PlacementDate")
                                            .setType("STRING"),
                                    new TableFieldSchema()
                                            .setName("ExitDate")
                                            .setType("STRING"),
                                    new TableFieldSchema()
                                            .setName("Location")
                                            .setType("STRING"),
                                    new TableFieldSchema()
                                            .setName("Building")
                                            .setType("STRING"),
                                    new TableFieldSchema()
                                            .setName("Room")
                                            .setType("STRING"),
                                    new TableFieldSchema()
                                            .setName("Semester")
                                            .setType("STRING"),
                                    new TableFieldSchema()
                                            .setName("Credits")
                                            .setType("NUMERIC"),
                                    new TableFieldSchema()
                                            .setName("ClassFormat")
                                            .setType("STRING"),
                                    new TableFieldSchema()
                                            .setName("SessionWeeks")
                                            .setType("STRING"),
                                    new TableFieldSchema()
                                            .setName("WeekDay")
                                            .setType("STRING"),
                                    new TableFieldSchema()
                                            .setName("StartTime")
                                            .setType("STRING"),
                                    new TableFieldSchema()
                                            .setName("EndTime")
                                            .setType("STRING"),
                                    new TableFieldSchema()
                                            .setName("Hours")
                                            .setType("NUMERIC")));

    rows.apply(
            "Write to BigQuery",
            BigQueryIO.writeTableRows()
                    .to(String.format("%s:%s.%s", "springml-arizona-oeo-ids", "ResearchProject1", "sample_records"))
                    .withSchema(tableSchemaJson)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                    .withCustomGcsTempLocation(ValueProvider.StaticValueProvider.of("gs://arizona-ids-dev-dataflow/temp")));

    p.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    runReadParquet();
  }
}
