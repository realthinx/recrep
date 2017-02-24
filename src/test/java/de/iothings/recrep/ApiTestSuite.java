package de.iothings.recrep;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;

/**
 * Created by ue60219 on 21.02.2017.
 */
@RunWith(VertxUnitRunner.class)
public class ApiTestSuite {

    @Rule
    public RunTestOnContext rule = new RunTestOnContext();

    @Before
    public void before(TestContext context) {
        List<String> deployableList = Collections.emptyList();
        RecrepEmbedded recrepEmbedded = new RecrepEmbedded(rule.vertx(), deployableList);
        recrepEmbedded.deploy(finished -> {
            System.out.println("Deployed");
        });
    }

    @Test
    public void test(TestContext context) {
        Async async = context.async();
    }
}
