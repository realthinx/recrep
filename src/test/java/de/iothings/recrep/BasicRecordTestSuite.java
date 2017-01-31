package de.iothings.recrep;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by ue60219 on 31.01.2017.
 */

@RunWith(VertxUnitRunner.class)
public class BasicRecordTestSuite {

    @Rule
    public RunTestOnContext rule = new RunTestOnContext();


    @Before
    public void before(TestContext context) {
        Async async = context.async();
        Vertx vertx = rule.vertx();
        List<String> deployableList = Collections.emptyList();
        RecrepEmbedded recrepEmbedded = new RecrepEmbedded(vertx, deployableList);
        recrepEmbedded.deploy(finished -> {
            async.complete();
        });
    }

    @Test
    public void testRecord(TestContext context) {
        context.assertFalse(false);
    }

    @After
    public void after(TestContext context) {
        rule.vertx().close(context.asyncAssertSuccess());
    }
}
