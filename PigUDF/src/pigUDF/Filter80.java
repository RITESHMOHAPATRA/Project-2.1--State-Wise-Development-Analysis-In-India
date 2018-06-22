package pigUDF;

import java.io.IOException;
import org.apache.pig.EvalFunc; 
import org.apache.pig.data.Tuple;

public class Filter80 extends EvalFunc<Double>{
	public Double exec(Tuple input) throws IOException {  
		double performance = (double)input.get(0);
		double objective = (double)input.get(1);
		
		double percentage = (performance/objective)*100;
		
		if(percentage>80.0) 
			return percentage;
		else
			return 0.0;	
	}
}

