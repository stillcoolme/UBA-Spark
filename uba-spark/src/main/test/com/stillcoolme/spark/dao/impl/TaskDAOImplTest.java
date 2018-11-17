package com.stillcoolme.spark.dao.impl; 

import com.stillcoolme.spark.dao.ITaskDao;
import com.stillcoolme.spark.domain.Task;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After; 

/** 
* TaskDaoImpl Tester.
* @since <pre>10/24/2018</pre> 
*/ 
public class TaskDAOImplTest { 

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
    * Method: findById(long taskid)
    */
    @Test
    public void testFindById() throws Exception {
        ITaskDao iTaskDAO = new TaskDaoImpl();
        Task task = iTaskDAO.findById(1);
        System.out.println(task.getTaskName());
    }


} 
