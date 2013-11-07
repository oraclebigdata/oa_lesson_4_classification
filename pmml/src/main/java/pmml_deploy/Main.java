/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package pmml_deploy;

import java.util.Properties;
import pattern.*;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.AssertionLevel;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.aggregator.Average;
import cascading.operation.aggregator.Count;
import cascading.operation.assertion.AssertMatches;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.operation.expression.*;


public class Main
  {

  /** @param args  */
  public static void main( String[] args ) throws RuntimeException
    {
	/* input paths */
	String pmmlPath = args[0];
	String weatherPath = args[1];
	String classifyPath = args[2];
	String trapPath = args[3];

	Fields inputFields = new Fields("stn", "wban", "weather_year", "weather_month", "weather_day", "temp", "dewp", "weather");
	
        /* Job properties */
	Properties properties = new Properties();
	AppProps.setApplicationJarClass(properties, Main.class);

	HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);
	// set up the input and output taps
	Tap weatherTap = new Hfs(new TextDelimited(inputFields, "\u0001"), weatherPath);
	Tap classifyTap = new Hfs(new TextDelimited(false, "\t"), classifyPath);
	Tap trapTap = new Hfs(new TextDelimited(false, "\t"), trapPath);
	String predictor = null;
	
	// set up the fields and the pipe
	Fields score = new Fields("score");
	ClassifierFunction classFunc = new ClassifierFunction(score,pmmlPath);
	Pipe classifyPipe = new Pipe("classify");
	// each observation gets scored by the pmml model
	classifyPipe = new Each(classifyPipe, classFunc.getInputFields(), classFunc, Fields.ALL);
	predictor = classFunc.getPredictor();
	ExpressionFilter predictorFilter = new ExpressionFilter("weather.equals(score)", String.class);
	classifyPipe = new Each(classifyPipe, Fields.ALL, predictorFilter);
	
	// put together a flow definition
	FlowDef flowDef = FlowDef.flowDef().setName("classify")
	    .addSource(classifyPipe, weatherTap)
            .addTrap(classifyPipe, trapTap)
            .addTailSink(classifyPipe, classifyTap);
            
	Flow classifyFlow = flowConnector.connect(flowDef);
	classifyFlow.writeDOT("dot/classify.dot");
	classifyFlow.complete();
	

    }
  }
