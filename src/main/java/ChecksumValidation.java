
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

public class ChecksumValidation extends GenericUDF {

  public ObjectInspectorConverters.Converter[] converters;

  CRC32 crc = new CRC32();

  String[] row = new String[8];

  public final String END = "end";

  public static final String START = "start";



  //Initialize the UDF for the checksum validation
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if(arguments.length != 8){
      throw new UDFArgumentException("The length of the args is wrong. Pls fix the query");
    }
    converters = new Converter[8];
    for(int i = 0; i < 8; i++){
      converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }
    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }

  @Override public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
    int result = 0;
    if(deferredObjects.length != 8){
      throw new HiveException("Wrong args");
    }
    for(int i = 0; i < 8; i++){
      try {
        row[i] = Text.decode(((Text) converters[i].convert(deferredObjects[i].get())).getBytes()).trim();
      } catch (CharacterCodingException e) {
        e.printStackTrace();
      }
    }

    int res = 0;
    if(!checkRowStart()){
      res ++;
    }
    if(!checkDataCrc()){
      res ++;
    }
//    if(!checkRowCheckSum()){
//      res = res + "3";
//    }
    if(!checkRowEnd()){
      res ++;
    }
    if(!checkRowEnd()){
      res ++;
    }
    if(!checkRowID()){
      res ++;
    }

    return new IntWritable(res);
  }

  public boolean checkRowID(){
    return row[1].equals(row[5]);
  }

  public boolean checkRowEnd(){
    return row[7].equals(END);
  }

  public boolean checkRowStart(){
    return row[0].equals(START);
  }

  //  val csvSchema: StructType = {
  //    new StructType().
  //        add("start", StringType). 0
  //        add("rowId", LongType). 1
  //        add("length", LongType). 2
  //        add("dataCrc", LongType). 3
  //        add("data", StringType). 4
  //        add("rowId2", LongType). 5
  //        add("rowCrc", LongType). 6
  //        add("end", StringType). 7
  //  }

  public boolean checkDataCrc(){
    crc.reset();
    crc.update(row[4].getBytes(StandardCharsets.UTF_8));
    return crc.getValue() == Long.parseLong(row[3]);
  }

  public boolean checkRowCheckSum() throws HiveException {
    crc.reset();
    for(int i = 0; i < 6; i++){
      crc.update(row[i].getBytes(StandardCharsets.UTF_8));
    }
    return crc.getValue() == Long.parseLong(row[6]);
  }

  @Override public String getDisplayString(String[] strings) {
    return null;
  }
}
