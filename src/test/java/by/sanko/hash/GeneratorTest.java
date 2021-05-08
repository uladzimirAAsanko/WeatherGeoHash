package by.sanko.hash;

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

public class GeneratorTest {

    @Test
    public void testGeneratorTrue(){
        String expected = "9qq6";
        String actual = Generator.generateGeoHash(35.6166,-114.834);
        assertEquals(actual, expected);
    }

    @Test
    public void testGeneratorFalse(){
        String expected = "9qq4";
        String actual = Generator.generateGeoHash(35.5837,-115.107);
        assertEquals(actual, expected);
    }
}
