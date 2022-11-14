package org.cruk.pipelines.referencegenomes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RecreateFastaTest
{
    RecreateFasta recreate = new RecreateFasta();
    File testDir;

    public RecreateFastaTest()
    {
        testDir = new File("target/fastatest");
    }

    @BeforeEach
    public void setup() throws IOException
    {
        FileUtils.deleteQuietly(testDir);
        FileUtils.forceMkdir(testDir);

        recreate.tempDir = testDir;
    }

    @AfterEach
    public void cleanup()
    {
        FileUtils.deleteQuietly(testDir);
    }

    @Test
    public void testExpandSourceFASTA_hg19() throws Exception
    {
        recreate.assembly = "hg19fromFASTA";
        recreate.inputFile = new File("src/test/data/hg19/fasta.txt.gz");

        try
        {
            recreate.expandSource();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail(e.getMessage());
        }

        assertRightNumber(93);
    }

    @Test
    public void testExpandSourceTAR_hg19() throws Exception
    {
        recreate.assembly = "hg19fromTAR";
        recreate.inputFile = new File("src/test/data/hg19/fasta.tar.gz");

        try
        {
            recreate.expandSource();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail(e.getMessage());
        }

        assertRightNumber(93);
    }

    @Test
    public void testExpandSourceFASTA_GRCh37() throws Exception
    {
        recreate.assembly = "GRCh37fromFASTA";
        recreate.inputFile = new File("src/test/data/GRCh37/fasta.txt.gz");

        try
        {
            recreate.expandSource();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail(e.getMessage());
        }

        assertRightNumber(84);
    }

    @Test
    public void testExpandSourceTAR_GRCh37() throws Exception
    {
        recreate.assembly = "GRCh37fromTAR";
        recreate.inputFile = new File("src/test/data/GRCh37/fasta.tar.gz");

        try
        {
            recreate.expandSource();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail(e.getMessage());
        }

        assertRightNumber(84);
    }

    private void assertRightNumber(int expected)
    {
        assertTrue(expected < 1000, "Too many expected files for unit test");

        File fastaDir = new File(recreate.workingDir, "000");

        assertEquals(expected, fastaDir.list().length, "Wrong number of files produced");
    }

    @Test
    public void testRecreate_hg19() throws Throwable
    {
        recreate.assembly = "hg19fromTAR";
        recreate.inputFile = new File("src/test/data/hg19/fasta.tar.gz");
        recreate.outputFile = new File(testDir, recreate.assembly + ".fa");

        recreate.contigOrder = new LinkedList<>();
        for (int c = 1; c <= 22; c++)
        {
            recreate.contigOrder.add("chr" + c);
        }
        recreate.contigOrder.add("chrX");
        recreate.contigOrder.add("chrY");
        recreate.contigOrder.add("chrM");

        List<String> expectedOrder = new LinkedList<>(recreate.contigOrder);

        expectedOrder.add("chr1_gl000191_random");
        expectedOrder.add("chr1_gl000192_random");
        expectedOrder.add("chr4_ctg9_hap1");
        expectedOrder.add("chr4_gl000193_random");
        expectedOrder.add("chr4_gl000194_random");
        expectedOrder.add("chr6_apd_hap1");
        expectedOrder.add("chr6_cox_hap2");
        expectedOrder.add("chr6_dbb_hap3");
        expectedOrder.add("chr6_mann_hap4");
        expectedOrder.add("chr6_mcf_hap5");
        expectedOrder.add("chr6_qbl_hap6");
        expectedOrder.add("chr6_ssto_hap7");
        expectedOrder.add("chr7_gl000195_random");
        expectedOrder.add("chr8_gl000196_random");
        expectedOrder.add("chr8_gl000197_random");
        expectedOrder.add("chr9_gl000198_random");
        expectedOrder.add("chr9_gl000199_random");
        expectedOrder.add("chr9_gl000200_random");
        expectedOrder.add("chr9_gl000201_random");
        expectedOrder.add("chr11_gl000202_random");
        expectedOrder.add("chr17_ctg5_hap1");
        expectedOrder.add("chr17_gl000203_random");
        expectedOrder.add("chr17_gl000204_random");
        expectedOrder.add("chr17_gl000205_random");
        expectedOrder.add("chr17_gl000206_random");
        expectedOrder.add("chr18_gl000207_random");
        expectedOrder.add("chr19_gl000208_random");
        expectedOrder.add("chr19_gl000209_random");
        expectedOrder.add("chr21_gl000210_random");
        expectedOrder.add("chrUn_gl000211");
        expectedOrder.add("chrUn_gl000212");
        expectedOrder.add("chrUn_gl000213");
        expectedOrder.add("chrUn_gl000214");
        expectedOrder.add("chrUn_gl000215");
        expectedOrder.add("chrUn_gl000216");
        expectedOrder.add("chrUn_gl000217");
        expectedOrder.add("chrUn_gl000218");
        expectedOrder.add("chrUn_gl000219");
        expectedOrder.add("chrUn_gl000220");
        expectedOrder.add("chrUn_gl000221");
        expectedOrder.add("chrUn_gl000222");
        expectedOrder.add("chrUn_gl000223");
        expectedOrder.add("chrUn_gl000224");
        expectedOrder.add("chrUn_gl000225");
        expectedOrder.add("chrUn_gl000226");
        expectedOrder.add("chrUn_gl000227");
        expectedOrder.add("chrUn_gl000228");
        expectedOrder.add("chrUn_gl000229");
        expectedOrder.add("chrUn_gl000230");
        expectedOrder.add("chrUn_gl000231");
        expectedOrder.add("chrUn_gl000232");
        expectedOrder.add("chrUn_gl000233");
        expectedOrder.add("chrUn_gl000234");
        expectedOrder.add("chrUn_gl000235");
        expectedOrder.add("chrUn_gl000236");
        expectedOrder.add("chrUn_gl000237");
        expectedOrder.add("chrUn_gl000238");
        expectedOrder.add("chrUn_gl000239");
        expectedOrder.add("chrUn_gl000240");
        expectedOrder.add("chrUn_gl000241");
        expectedOrder.add("chrUn_gl000242");
        expectedOrder.add("chrUn_gl000243");
        expectedOrder.add("chrUn_gl000244");
        expectedOrder.add("chrUn_gl000245");
        expectedOrder.add("chrUn_gl000246");
        expectedOrder.add("chrUn_gl000247");
        expectedOrder.add("chrUn_gl000248");
        expectedOrder.add("chrUn_gl000249");

        testOrdered(expectedOrder);
    }

    @Test
    public void testRecreate_GRCh37() throws Throwable
    {
        recreate.assembly = "GRCh37fromFASTA";
        recreate.inputFile = new File("src/test/data/GRCh37/fasta.txt.gz");
        recreate.outputFile = new File(testDir, recreate.assembly + ".fa");

        // Reverse order, to make sure they're not sorting numerically.

        recreate.contigOrder = new LinkedList<>();
        for (int c = 22; c >= 1; c--)
        {
            recreate.contigOrder.add(Integer.toString(c));
        }
        recreate.contigOrder.add("X");
        recreate.contigOrder.add("Y");
        recreate.contigOrder.add("MT");

        List<String> expectedOrder = new LinkedList<>(recreate.contigOrder);

        expectedOrder.add("GL000191.1");
        expectedOrder.add("GL000192.1");
        expectedOrder.add("GL000193.1");
        expectedOrder.add("GL000194.1");
        expectedOrder.add("GL000195.1");
        expectedOrder.add("GL000196.1");
        expectedOrder.add("GL000197.1");
        expectedOrder.add("GL000198.1");
        expectedOrder.add("GL000199.1");
        expectedOrder.add("GL000200.1");
        expectedOrder.add("GL000201.1");
        expectedOrder.add("GL000202.1");
        expectedOrder.add("GL000203.1");
        expectedOrder.add("GL000204.1");
        expectedOrder.add("GL000205.1");
        expectedOrder.add("GL000206.1");
        expectedOrder.add("GL000207.1");
        expectedOrder.add("GL000208.1");
        expectedOrder.add("GL000209.1");
        expectedOrder.add("GL000210.1");
        expectedOrder.add("GL000211.1");
        expectedOrder.add("GL000212.1");
        expectedOrder.add("GL000213.1");
        expectedOrder.add("GL000214.1");
        expectedOrder.add("GL000215.1");
        expectedOrder.add("GL000216.1");
        expectedOrder.add("GL000217.1");
        expectedOrder.add("GL000218.1");
        expectedOrder.add("GL000219.1");
        expectedOrder.add("GL000220.1");
        expectedOrder.add("GL000221.1");
        expectedOrder.add("GL000222.1");
        expectedOrder.add("GL000223.1");
        expectedOrder.add("GL000224.1");
        expectedOrder.add("GL000225.1");
        expectedOrder.add("GL000226.1");
        expectedOrder.add("GL000227.1");
        expectedOrder.add("GL000228.1");
        expectedOrder.add("GL000229.1");
        expectedOrder.add("GL000230.1");
        expectedOrder.add("GL000231.1");
        expectedOrder.add("GL000232.1");
        expectedOrder.add("GL000233.1");
        expectedOrder.add("GL000234.1");
        expectedOrder.add("GL000235.1");
        expectedOrder.add("GL000236.1");
        expectedOrder.add("GL000237.1");
        expectedOrder.add("GL000238.1");
        expectedOrder.add("GL000239.1");
        expectedOrder.add("GL000240.1");
        expectedOrder.add("GL000241.1");
        expectedOrder.add("GL000242.1");
        expectedOrder.add("GL000243.1");
        expectedOrder.add("GL000244.1");
        expectedOrder.add("GL000245.1");
        expectedOrder.add("GL000246.1");
        expectedOrder.add("GL000247.1");
        expectedOrder.add("GL000248.1");
        expectedOrder.add("GL000249.1");

        testOrdered(expectedOrder);
    }

    private void testOrdered(List<String> expectedOrder) throws Throwable
    {
        try
        {
            recreate.expandSource();

            assertRightNumber(expectedOrder.size());

            recreate.writeFasta();

            assertTrue(recreate.outputFile.exists(), "Output FASTA not created");

            Iterator<String> expectedIter = expectedOrder.iterator();
            try (BufferedReader reader = new BufferedReader(new FileReader(recreate.outputFile)))
            {
                String line;
                while ((line = reader.readLine()) != null)
                {
                    Matcher m = RecreateFasta.HEADER_LINE.matcher(line);
                    if (m.matches())
                    {
                        assertEquals(expectedIter.next(), m.group(1), "Contig name read is what was expected");
                    }
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
