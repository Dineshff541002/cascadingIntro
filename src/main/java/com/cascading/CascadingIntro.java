package com.cascading;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlow;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Debug;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;


public class CascadingIntro {
    //defining delimiters and file locations
    private static String DELIMITER_SOURCE = ",";
    private static String DELIMITER_SINK = "|";
    private static String FILE_SOURCE = "/cascadingIntro/cascading_intro_file.csv";
    private static String FILE_SINK = "cascading_intro_file_sink.csv";
    private static FlowDef flowDef = new FlowDef();



    public static void main(String[] args){
//        FILE_SOURCE = args[0];
//        FILE_SINK = args[1];
        //fields that match our data layout
        Fields sourceFields = new Fields("id","first_name", "last_name","email","gender","salary");
        //source and sink taps definition
        Tap sourceTap = new Hfs(new TextDelimited(sourceFields, true, DELIMITER_SOURCE), FILE_SOURCE);
        Tap sinkTap = new Hfs(new TextDelimited(sourceFields, false, DELIMITER_SINK), FILE_SINK, SinkMode.REPLACE);
        Pipe pipe = new Pipe("personData");
        pipe = new Each(pipe, new Debug("pipeLayout", true));
        //completing the flow
        flowDef.addSource(pipe, sourceTap);
        flowDef.addTailSink(pipe, sinkTap);
        Flow flow = new HadoopFlowConnector().connect(flowDef);
        flow.complete();
    }
}


