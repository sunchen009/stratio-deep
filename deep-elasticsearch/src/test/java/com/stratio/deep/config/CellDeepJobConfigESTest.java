package com.stratio.deep.config;

import com.stratio.deep.entity.Cells;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Created by dgomez on 20/08/14.
 */
@Test
public class CellDeepJobConfigESTest {

    @Test
    public void createTest() {

        GenericDeepJobConfigES<Cells> cellDeepJobConfigES = new CellDeepJobConfigES();

        assertNotNull(cellDeepJobConfigES);

        assertEquals(cellDeepJobConfigES.getEntityClass(), Cells.class);

    }
}