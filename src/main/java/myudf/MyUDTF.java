package myudf;

import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

// TODO define input and output types, e.g. "string,string->string,bigint".
@Resolve({"string,string->string,bigint"})
public class MyUDTF extends UDTF {

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {

    }

    @Override
    public void process(Object[] args) throws UDFException {
        String a = (String) args[0];
        long b = args[1] == null ? 0 : ((String) args[1]).length();

        forward(a, b);
    }

    @Override
    public void close() throws UDFException {

    }

}