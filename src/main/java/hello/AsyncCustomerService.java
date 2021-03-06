package hello;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class AsyncCustomerService {
  
  private static final Logger log = LoggerFactory.getLogger(AsyncCustomerService.class);
  
  @Autowired
  JdbcTemplate jdbcTemplate;
  
//  @Autowired
//  ThreadPoolTaskExecutor taskExecutor;
  
  @Transactional(isolation=Isolation.READ_COMMITTED)
  public void createTable() 
  throws Exception {
    log.info("createTable() start");
    
    try {
      try {
        jdbcTemplate.execute("drop table common.customers");
      } catch (Exception e) {
        log.error("Failed to drop table. Continue processing.");
      }
      
      jdbcTemplate.execute("create table common.customers ("
          + "ID BIGINT  NOT NULL  GENERATED ALWAYS AS IDENTITY (START WITH 1001, INCREMENT BY 1, NO CACHE ) , "
          + "FIRST_NAME VARCHAR(255) , "
          + "LAST_NAME VARCHAR(255) , "
          + "PRIMARY KEY (ID)"
          + ")");
      
    } catch (DataAccessException e) {
      log.error("createTable(): DataAccessException", e);
      throw e;
    }
    
    log.info("createTable() end");
    
    return;
  }
  
  public static void insertDataAsync(List<Object[]> splitNames, AsyncCustomerService asyncCustomerService) 
  throws Exception {
    log.info("insertDataAsync() start");
    
    List<CompletableFuture<Boolean>> taskList = new ArrayList<CompletableFuture<Boolean>>();
    CompletableFuture<Boolean> cf = null;
    
    cf = asyncCustomerService.insertData(splitNames.subList(0, 2));
    taskList.add(cf);
    
    cf = asyncCustomerService.insertData(splitNames.subList(2, 4));
    taskList.add(cf);
    
    for (int i = 0; i < taskList.size(); i++) {
      cf = taskList.get(i);
      // blocks current thread until result is returned by tempTask
      cf.join();
    }
    
    log.info("insertDataAsync() end");
  }

  @Transactional(isolation=Isolation.READ_COMMITTED)
  @Async
  public CompletableFuture<Boolean> insertData(List<Object[]> splitNames) 
  throws Exception {
    log.info("insertData() start");
    
    Boolean result = Boolean.FALSE;
    final String strSqlInsert = "insert into common.customers(FIRST_NAME, LAST_NAME) values (?, ?)";

    try {
      splitNames.forEach(name -> log.info(String.format("Working data: %s, %s", name[0], name[1])));
      
      int[][] updateCounts = jdbcTemplate.batchUpdate(
          strSqlInsert, splitNames, 3, 
          new ParameterizedPreparedStatementSetter<Object[]>() {

            @Override
            public void setValues(PreparedStatement ps, Object[] argument) throws SQLException {
              int i = 1, j = 0;
              ps.setString(i++, String.valueOf((argument[j++])));
              ps.setString(i++, String.valueOf((argument[j++])));
            }
            
          });
      log.info(String.format("Update counts: %s", Arrays.deepToString(updateCounts)));
      result = Boolean.TRUE;
    } catch (DataAccessException e) {
      if (e.contains(BatchUpdateException.class)) {
        SQLException be = (SQLException)e.getCause();
        do {
          log.error("insertData(): SQLException", be);
          be = be.getNextException();
        } while (be != null);
      } else {
        log.error("insertData(): DataAccessException", e);
      }
      throw e;
    } catch (Exception e) {
      log.error("insertData(): Exception", e);
      throw e;
    }
    
    log.info("insertData() end");
    
    return CompletableFuture.completedFuture(result);
  }
  
  
  @Transactional(propagation=Propagation.SUPPORTS)
  public void findByFirstName(String firstName)
  throws Exception {
    log.info("findByFirstName() start");
    
    try {
      final String strSqlSelect = "select * from common.customers where FIRST_NAME = ?";
      
      jdbcTemplate.query(
          strSqlSelect, new Object[] { firstName },
          (rs, rowNo) -> new Customer(rs.getLong("id"), rs.getString("first_name"), rs.getString("last_name"))
        ).forEach(customer -> log.info(customer.toString()));
    } catch (Exception e) {
      log.error("findByFirstName(): Exception", e);
      throw e;
    }
    
    log.info("findByFirstName() end");
  }
  
}
