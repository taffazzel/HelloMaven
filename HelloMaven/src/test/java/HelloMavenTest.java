import static org.junit.Assert.*;

/**
 * Created by tafaz on 8/8/2017.
 */
public class HelloMavenTest {
    @org.junit.Test
    public void getName1() throws Exception {
        HelloMaven hm = new HelloMaven("HelloMaven");
	HelloMaven hm2 = new HelloMaven("Hello");
	HelloMaven hm3 = new HelloMaven("World")
        assertEquals("HelloMaven", hm.getName());
	assertEquals("Hello", hm2.getName());
	assertEquals("World",hm3.getName());

    }

}
