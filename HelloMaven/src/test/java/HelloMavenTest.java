import static org.junit.Assert.*;

/**
 * Created by tafaz on 8/8/2017.
 */
public class HelloMavenTest {
    @org.junit.Test
    public void getName() throws Exception {
        HelloMaven hm = new HelloMaven("HelloMaven");
        assertEquals("HelloMaven", hm.getName());
    }

}