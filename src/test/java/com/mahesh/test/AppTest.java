package com.mahesh.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.http.util.Asserts;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

public class AppTest {
    private static final String PARQUET_FILE_PATH = "/tmp/a.parquet";

    @Before
    public void beforeTest() throws Exception {
        Files.deleteIfExists(Paths.get(PARQUET_FILE_PATH));
    }

    @After
    public void afterTest() throws Exception {
        Files.deleteIfExists(Paths.get(PARQUET_FILE_PATH));
    }

    private String writeDemoData(String[] testNames) throws Exception {
        MessageType schema = Types.buildMessage().
                required(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("name")
                .named("msg");

        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);

        GroupFactory factory = new SimpleGroupFactory(schema);
        Path path = new Path(PARQUET_FILE_PATH);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
                .withConf(conf)
                .withPageRowCountLimit(10)
                .withDictionaryEncoding(true)
                .withBloomFilterEnabled("name", true)
                .build()) {
            for (String testName : testNames) {
                writer.write(factory.newGroup().append("name", testName));
            }
        }
        return "/tmp/a.parquet";
    }

    private BloomFilter createFileAndGetBloomFilter(String[] testNames) throws Exception {
        ParquetFileReader reader =
                ParquetFileReader.open(HadoopInputFile.fromPath(new Path(new URI(writeDemoData(testNames))),
                        new Configuration()));
        BlockMetaData blockMetaData = reader.getFooter().getBlocks().get(0);
        return reader.getBloomFilterDataReader(blockMetaData).readBloomFilter(blockMetaData.getColumns().get(0));
    }

    @Test
    public void testWritingDistinctValues() throws Exception {
        String[] testData = {"data1", "data2", "data3", "data4"};
        BloomFilter bloomFilter = createFileAndGetBloomFilter(testData);
        System.out.println(String.format("Bloomfilter Data = %s", bloomFilter));
        Asserts.notNull(bloomFilter, "Bloomfilter");
    }

    @Test
    public void testWritingDuplicateValues() throws Exception {
        String[] testData = {"data1", "data1", "data1", "data1"};
        BloomFilter bloomFilter = createFileAndGetBloomFilter(testData);
        System.out.println(String.format("Bloomfilter Data = %s", bloomFilter));
        Asserts.notNull(bloomFilter, "Bloomfilter");
    }
}
