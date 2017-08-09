import static org.junit.Assert.*;

/**
 * Created by tafaz on 8/8/2017.
 */
public class HelloMavenTest {
    @org.junit.Test
    public void getName() throws Exception {
        HelloMaven hm1 = new HelloMaven("HelloMaven");
	HelloMaven hm2 = new HelloMaven("Hello");
	HelloMaven hm3 = new HelloMaven("World");
        assertEquals("HelloMaven", hm1.getName());
	assertEquals("Hell", hm2.getName());
	assertEquals("Word",hm3.getName());
    }

}
