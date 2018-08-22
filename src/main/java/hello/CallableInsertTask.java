package hello;

import java.util.List;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CallableInsertTask implements Callable<Boolean> {
  
  private static final Logger log = LoggerFactory.getLogger(CallableInsertTask.class);
  
  List<Object[]> workingList;
  
  ConcurrentCustomerService conCustomerService;

  public CallableInsertTask(List<Object[]> workingList, ConcurrentCustomerService conCustomerService) {
    super();
    this.workingList = workingList;
    this.conCustomerService = conCustomerService;
  }
  
  @Override
  public Boolean call() throws Exception {
    
    try {
      conCustomerService.insertData(workingList);
    } catch (Exception e) {
      log.error("call(): Exception", e);
      return Boolean.FALSE;
    } 
    
    return Boolean.TRUE;
  }
  
}