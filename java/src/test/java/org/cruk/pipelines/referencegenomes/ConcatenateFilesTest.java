package org.cruk.pipelines.referencegenomes;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.StringUtils.repeat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.cruk.common.compression.CompressionOutputStreamBuilder;
import org.cruk.common.compression.CompressionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConcatenateFilesTest
{
    ConcatenateFiles concatenateFiles;
    File testDir;

    List<File> files;
    File outFile;
    File checksumFile;

    StringBuilder sb;
    byte[] zipBytes;

    CompressionOutputStreamBuilder cstreamBuilder;

    public ConcatenateFilesTest()
    {
        testDir = new File("target/concattest");

        cstreamBuilder = CompressionOutputStreamBuilder.newInstance()
                .withCompressionMethod(CompressorStreamFactory.GZIP)
                .withCompressionLevel(1);
    }

    @BeforeEach
    public void setup() throws IOException
    {
        FileUtils.deleteDirectory(testDir);
        FileUtils.forceMkdir(testDir);

        files = new ArrayList<>(8);
        sb = new StringBuilder();

        GzipParameters params = new GzipParameters();
        params.setCompressionLevel(1);

        ByteArrayOutputStream zipByteStream = new ByteArrayOutputStream(8192);
        OutputStream zipStream = new GzipCompressorOutputStream(zipByteStream, params);

        for (int i = 0; i < 8; i++)
        {
            File f = new File(testDir, "f" + i + ".txt");

            writeFile(f, repeat(Integer.toString(i), 40));

            sb.append(FileUtils.readFileToString(f, UTF_8));
            zipStream.write(FileUtils.readFileToByteArray(f));

            files.add(f);
        }

        zipStream.close();
        zipBytes = zipByteStream.toByteArray();

        outFile = new File(testDir, "cat.txt.pipetemp");
        checksumFile = new File(testDir, "cat.txt.md5");

        concatenateFiles = new ConcatenateFiles();
        concatenateFiles.algorithm = "MD5";
        concatenateFiles.suffix = ".pipetemp";
        concatenateFiles.inputFiles = Collections.unmodifiableList(files);
        concatenateFiles.outputFile = outFile;
    }

    @AfterEach
    public void cleanup() throws IOException
    {
        FileUtils.deleteDirectory(testDir);
    }

    @Test
    public void noCompression() throws Exception
    {
        concatenateFiles.zip = false;

        int code = concatenateFiles.call();
        assertEquals(0, code, "Return state wrong");

        assertEquals(sb.toString(), FileUtils.readFileToString(outFile, UTF_8), "Concatenated file wrong");
    }

    @Test
    public void withZip() throws Exception
    {
        concatenateFiles.zip = true;

        int code = concatenateFiles.call();
        assertEquals(0, code, "Return state wrong");

        assertArrayEquals(zipBytes, FileUtils.readFileToByteArray(outFile), "Concatenated file wrong");
    }

    @Test
    public void withZipAndChecksum() throws Exception
    {
        concatenateFiles.zip = true;
        concatenateFiles.checksumFile = checksumFile;

        int code = concatenateFiles.call();
        assertEquals(0, code, "Return state wrong");

        assertArrayEquals(zipBytes, FileUtils.readFileToByteArray(outFile), "Concatenated file wrong");

        assertEquals("125869b9c990abdcc8b715476a5cb12d  cat.txt", FileUtils.readLines(checksumFile, UTF_8).get(0).toString(), "Concatenated file checksum wrong");
    }

    @Test
    public void withZippedSourceFiles() throws Exception
    {
        concatenateFiles.zip = true;
        concatenateFiles.checksumFile = checksumFile;

        replaceWithZipped(2, 5);

        assertFalse(CompressionUtils.isCompressed(files.get(1)), files.get(1).getName() + " should not be zipped");
        assertTrue(CompressionUtils.isCompressed(files.get(2)), files.get(2).getName() + " should be zipped");

        File outFile = new File(testDir, "cat.txt.pipetemp");
        File checksumFile = new File(testDir, "cat.txt.md5");

        concatenateFiles.outputFile = outFile;
        concatenateFiles.inputFiles = Collections.unmodifiableList(files);
        concatenateFiles.zip = true;
        concatenateFiles.checksumFile = checksumFile;

        int code = concatenateFiles.call();
        assertEquals(0, code, "Return state wrong");

        assertArrayEquals(zipBytes, FileUtils.readFileToByteArray(outFile), "Concatenated file wrong");

        assertEquals("125869b9c990abdcc8b715476a5cb12d  cat.txt", FileUtils.readLines(checksumFile, UTF_8).get(0).toString(), "Concatenated file checksum wrong");
    }

    private void writeFile(File file, String toPrint) throws IOException
    {
        Writer writer = new FileWriterWithEncoding(file, UTF_8);

        writer.append(toPrint);
        writer.append('\n');

        writer.close();
    }

    private void replaceWithZipped(int... indexes) throws IOException, CompressorException
    {
        for (int index : indexes)
        {
            files.set(index, zipFile(files.get(index)));
        }
    }

    private File zipFile(File file) throws IOException, CompressorException
    {
        File zipped = new File(file.getParentFile(), file.getName() + ".gz");

        InputStream in = new FileInputStream(file);
        OutputStream out = cstreamBuilder.addCompressionStream(new FileOutputStream(zipped));

        IOUtils.copy(in, out);

        out.close();
        in.close();

        return zipped;
    }
}
